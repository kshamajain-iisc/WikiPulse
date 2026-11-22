/* Scoped playback clock. Event-time advancement is independent of query latency. */
(function () {
  window.dash_clientside = window.dash_clientside || {};
  window.dash_clientside.pulse = {
    clock: function (tick, speed, start, seek, mode, kind, day, zone, seed, path, previous, refreshSeconds, engine) {
      const dc = window.dash_clientside;
      const changed = ((dc.callback_context || {}).triggered || []).map(x => x.prop_id.split('.')[0]);
      const now = Date.now();
      const key = JSON.stringify([mode, kind, day, zone]);
      let s = previous ? Object.assign({}, previous) : {cursor: 0, playing: false, speed: speed || 50, at: now};
      const replay = mode === 'stored' && kind === 'replay';
      const active = path === '/' || path === '/investigation';
      const scopeChanged = s.key !== key;
      const wasPlaying = s.playing;
      if (s.playing) s.cursor = Math.min(86400, s.cursor + Math.max(0, now - s.at) / 1000 * s.speed);
      if (scopeChanged) {
        s.cursor = 0; s.playing = false; s.stopped = false;
      }
      if (changed.includes('pulse-seed') && seed && seed.url !== s.seed) {
        // Links capture a stable replay position. Ordinary refreshes do not reset it.
        s.cursor = Number(seed.cursor) || 0; s.seed = seed.url;
      }
      if (changed.includes('pulse-seek') && Number.isFinite(Number(seek))) s.cursor = Number(seek);
      if (changed.includes('pulse-replay-engine')) {s.playing = false;s.stopped = false;}
      if (changed.includes('pulse-play') && engine !== 'kafka') {
        if (s.cursor === 0 || s.cursor >= 86400) s.cursor = Number(seed && seed.first_cursor) || 0;
        s.playing = true; s.started = true; s.stopped = false;
      }
      if (changed.includes('pulse-cancel')) {s.playing = false; s.stopped = true;}
      if (seed && seed.kafka_cursor !== undefined) { s.cursor=Number(seed.kafka_cursor)||0; s.playing=false; }
      if (!replay || !active || s.cursor >= 86400) s.playing = false;
      s.cursor = Math.max(0, Math.min(86400, s.cursor));
      s.speed = Number(speed) || 1; s.at = now; s.key = key;
      const sec = Math.floor(s.cursor);
      const text = [Math.floor(sec/3600),Math.floor(sec/60)%60,sec%60].map(n => String(n).padStart(2,'0')).join(':');
      const remaining = Math.ceil((86400-s.cursor)/s.speed);
      const eta = seed&&seed.kafka_cursor!==undefined ? `Kafka run ${seed.kafka_status} · showing Spark-committed output` :
        s.cursor >= 86400 ? 'Day complete' : `${Math.floor(remaining/60)} min ${remaining%60} sec remaining at ${s.speed}×`;
      // A smooth clock is independent of the much slower chart refresh cadence.
      const refresh = [10,30,60].includes(Number(refreshSeconds)) ? Number(refreshSeconds) : 30;
      if (changed.includes('pulse-refresh-rate')) s.frameAt = now;
      const kafka = seed && seed.kafka_cursor !== undefined;
      const terminal = kafka && ['complete','cancelled','failed'].includes(seed.kafka_status);
      const finishedKafka = terminal && s.kafkaStatus !== seed.kafka_status;
      const immediate = scopeChanged || finishedKafka || (wasPlaying && s.cursor >= 86400) || changed.some(
        x => ['pulse-seek','pulse-seed','pulse-play','pulse-cancel','pulse-replay-engine'].includes(x));
      const automatic = active && (mode === 'live' || replay && (s.playing || kafka && !terminal && seed.kafka_status !== 'ready'));
      const due = s.frameAt === undefined || now - s.frameAt >= refresh * 1000;
      let frame = dc.no_update;
      if (active && (immediate || automatic && due)) {
        s.frameAt = now; s.frames = (s.frames || 0) + 1;
        frame = {cursor:s.cursor, render:mode === 'live' || replay, automatic:!immediate, token:key+':'+s.frames};
      }
      s.kafkaStatus = kafka ? seed.kafka_status : null;
      const status = kafka ? 'Kafka & Spark · '+(seed.kafka_status==='cancelled'?'Stopped':seed.kafka_status) :
        'Saved playback · '+(s.cursor>=86400?'Complete':s.playing?'Running':s.stopped?'Stopped':'Ready');
      return [s, frame, text + ' ' + (zone || 'IST'), status, s.cursor, eta];
    }
  };
})();
