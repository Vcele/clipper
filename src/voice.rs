use {
  crate::composer::{DirectorImplementation, CHANNELS, SAMPLING_RATE},
  serenity::{
    async_trait,
    client::Context,
    model::prelude::{ChannelId, Guild, GuildId, VoiceState},
    prelude::EventHandler,
  },
  songbird::{CoreEvent, Event, EventContext, EventHandler as VoiceEventHandler, Songbird},
  std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
  },
};

#[derive(Debug)]
pub enum VoiceError {
  GuildNotFound,
  SongbirdInitError,
  SongbirdConnectError,
}

#[derive(Clone)]
struct Receiver {
  director: DirectorImplementation,
  guild_id: GuildId,
}

impl Receiver {
  pub fn new(guild_id: GuildId, director: DirectorImplementation) -> Self {
    Self { director, guild_id }
  }
}

#[async_trait]
impl VoiceEventHandler for Receiver {
  async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
    if let EventContext::VoiceTick(tick) = ctx {
      let voices = tick
        .speaking
        .values()
        .filter_map(|data| data.decoded_voice.as_deref());

      self
        .director
        .lock()
        .unwrap()
        .incoming_tick(&self.guild_id, voices);
    }

    None
  }
}

pub fn save_clip(guild_id: &GuildId, data: &[i16]) -> Result<String, hound::Error> {
  let output_dir = format!("output/{}", guild_id);
  let path = format!(
    "{}/{}.wav",
    output_dir,
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis()
  );

  std::fs::create_dir_all(&output_dir)?;

  let spec = hound::WavSpec {
    channels: CHANNELS,
    sample_rate: SAMPLING_RATE,
    bits_per_sample: 16,
    sample_format: hound::SampleFormat::Int,
  };
  let mut writer = hound::WavWriter::create(&path, spec)?;
  for sample in data {
    writer.write_sample(*sample)?;
  }
  writer.finalize()?;

  Ok(path)
}

pub struct Handler {
  director: DirectorImplementation,
}

impl Handler {
  pub fn new(director: DirectorImplementation) -> Self {
    Handler { director }
  }

  async fn update_guild(&self, ctx: &Context, guild_id: GuildId) {
    let update = match get_guild_voice_update(ctx, guild_id) {
      Ok(v) => v,
      Err(e) => {
        println!("Encountered voice error. {:?}", e);
        return;
      }
    };

    if let GuildVoiceState::Nothing = update {
      return;
    }

    println!("Update {:?}", update);

    if let Err(err) = handle_guild_voice_state(ctx, &update, self.director.clone()).await {
      println!("{:?}", err);
    }
  }
}

#[async_trait]
impl EventHandler for Handler {
  // join channels that already have people in them when the bot starts
  async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: Option<bool>) {
    self.update_guild(&ctx, guild.id).await;
  }

  async fn voice_state_update(&self, ctx: Context, _old: Option<VoiceState>, new: VoiceState) {
    if let Some(guild_id) = new.guild_id {
      self.update_guild(&ctx, guild_id).await;
    }
  }
}

#[derive(Debug)]
pub enum GuildVoiceState {
  Leave(GuildId),
  Join(GuildId, ChannelId),
  Move(GuildId, ChannelId),
  Nothing,
}

fn get_guild_voice_update(ctx: &Context, guild_id: GuildId) -> Result<GuildVoiceState, VoiceError> {
  let bot_id = ctx.cache.current_user().id;
  let guild = ctx.cache.guild(guild_id).ok_or(VoiceError::GuildNotFound)?;

  let mut bot_channel: Option<ChannelId> = None;
  let mut members_per_channel: HashMap<ChannelId, usize> = HashMap::new();

  for (user_id, state) in guild.voice_states.iter() {
    let Some(channel_id) = state.channel_id else {
      continue;
    };

    if *user_id == bot_id {
      bot_channel = Some(channel_id);
    } else {
      *members_per_channel.entry(channel_id).or_default() += 1;
    }
  }

  if let Some(channel_id) = bot_channel {
    if members_per_channel.contains_key(&channel_id) {
      return Ok(GuildVoiceState::Nothing);
    }
  }

  // pick the busiest channel, lowest id on ties so the choice is stable
  let target = members_per_channel
    .into_iter()
    .max_by(|(a_id, a_count), (b_id, b_count)| a_count.cmp(b_count).then(b_id.cmp(a_id)))
    .map(|(id, _)| id);

  Ok(match (target, bot_channel) {
    (Some(id), Some(_)) => GuildVoiceState::Move(guild_id, id),
    (Some(id), None) => GuildVoiceState::Join(guild_id, id),
    (None, Some(_)) => GuildVoiceState::Leave(guild_id),
    (None, None) => GuildVoiceState::Nothing,
  })
}

async fn handle_guild_voice_state(
  ctx: &Context,
  update: &GuildVoiceState,
  director: DirectorImplementation,
) -> Result<(), VoiceError> {
  match update {
    GuildVoiceState::Nothing => {}
    GuildVoiceState::Join(guild_id, channel_id) | GuildVoiceState::Move(guild_id, channel_id) => {
      director.lock().unwrap().join(guild_id);
      join_voice_channel(ctx, guild_id, channel_id, director.clone()).await?;
    }
    GuildVoiceState::Leave(guild_id) => {
      director.lock().unwrap().leave(guild_id);
      leave_voice_channel(ctx, guild_id).await?;
    }
  }

  Ok(())
}

async fn join_voice_channel(
  ctx: &Context,
  guild_id: &GuildId,
  channel_id: &ChannelId,
  director: DirectorImplementation,
) -> Result<(), VoiceError> {
  let manager = create_songbird_manager(ctx).await?;

  let handler_lock = match manager.join(*guild_id, *channel_id).await {
    Ok(v) => v,
    Err(e) => {
      println!("Error: {}", e);
      return Err(VoiceError::SongbirdConnectError);
    }
  };

  let mut handler = handler_lock.lock().await;

  if handler.deafen(false).await.is_err() {
    println!("Was unable to set deaf status");
  }

  handler.remove_all_global_events();
  handler.add_global_event(
    CoreEvent::VoiceTick.into(),
    Receiver::new(*guild_id, director),
  );

  Ok(())
}

async fn leave_voice_channel(ctx: &Context, guild_id: &GuildId) -> Result<(), VoiceError> {
  let manager = create_songbird_manager(ctx).await?;

  if manager.get(*guild_id).is_some() && manager.remove(*guild_id).await.is_err() {
    return Err(VoiceError::SongbirdConnectError);
  }

  Ok(())
}

async fn create_songbird_manager(ctx: &Context) -> Result<Arc<Songbird>, VoiceError> {
  songbird::get(ctx)
    .await
    .ok_or(VoiceError::SongbirdInitError)
}
