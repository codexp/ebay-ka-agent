'use strict';

const { getDb } = require('../db');
const { scrapeSource } = require('../scraper');
const { acquire, ping, complete, PING_INTERVAL_MS } = require('../scraper/locks');

const MIN_INTERVAL = parseInt(process.env.MIN_SCRAPE_INTERVAL_MINUTES || '5', 10);
const BUSY_RECHECK_MS = parseInt(process.env.SCRAPE_BUSY_RECHECK_MS || '30000', 10);
const jobs = new Map();

function getEffectiveIntervalMinutes(source) {
  const interval = parseInt(source?.interval, 10);
  return Math.max(Number.isFinite(interval) ? interval : MIN_INTERVAL, MIN_INTERVAL);
}

function getLastFetchMs(source) {
  if (!source?.last_scraped_at) return null;
  const parsed = Date.parse(source.last_scraped_at);
  return Number.isFinite(parsed) ? parsed : null;
}

function getNextRunAt(source, nowMs = Date.now()) {
  const lastFetchMs = getLastFetchMs(source);
  if (lastFetchMs === null) return new Date(nowMs);
  return new Date(lastFetchMs + getEffectiveIntervalMinutes(source) * 60 * 1000);
}

function getNextRunDelayMs(source, nowMs = Date.now()) {
  return Math.max(0, getNextRunAt(source, nowMs).getTime() - nowMs);
}

function formatTimeForLog(value) {
  if (!value) return 'never';
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return String(value);
  return new Date(parsed).toISOString();
}

function loadSource(sourceId) {
  return getDb().prepare('SELECT * FROM sources WHERE id = ?').get(sourceId);
}

function setJobTimer(job, delayMs) {
  job.timer = setTimeout(() => runJob(job.sourceId, job), delayMs);
  if (typeof job.timer.unref === 'function') job.timer.unref();
}

function scheduleNext(source, job) {
  if (job.stopped) return;

  const nowMs = Date.now();
  const interval = getEffectiveIntervalMinutes(source);
  const nextRunAt = getNextRunAt(source, nowMs);
  const delayMs = getNextRunDelayMs(source, nowMs);

  console.log(
    `[scheduler] Source ${source.id}: last fetch ${formatTimeForLog(source.last_scraped_at)}; ` +
    `interval ${interval} min; next fetch ${nextRunAt.toISOString()}`
  );
  setJobTimer(job, delayMs);
}

async function runJob(sourceId, job) {
  if (job.stopped) return;

  const source = loadSource(sourceId);
  if (!source || !source.active) {
    removeJob(sourceId);
    return;
  }

  const delayMs = getNextRunDelayMs(source);
  if (delayMs > 0) {
    scheduleNext(source, job);
    return;
  }

  if (!acquire(source.id)) {
    console.log(`[scheduler] source ${source.id} scrape still running, rechecking in ${Math.ceil(BUSY_RECHECK_MS / 1000)}s`);
    setJobTimer(job, BUSY_RECHECK_MS);
    return;
  }

  console.log(`[scheduler] Scraping source ${source.id} (${source.name})`);
  const pingInterval = setInterval(() => ping(source.id), PING_INTERVAL_MS);
  let failed = false;
  try {
    const stats = await scrapeSource(source.id);
    complete(source.id, stats, null);
    console.log(`[scheduler] Source ${source.id} done:`, stats);
  } catch (err) {
    failed = true;
    complete(source.id, null, err.message);
    console.error(`[scheduler] Source ${source.id} error:`, err.message);
  } finally {
    clearInterval(pingInterval);
    const refreshed = loadSource(source.id);
    if (refreshed?.active && !job.stopped) {
      if (failed) {
        setJobTimer(job, getEffectiveIntervalMinutes(refreshed) * 60 * 1000);
      } else {
        scheduleNext(refreshed, job);
      }
    }
  }
}

function addJob(source) {
  if (jobs.has(source.id)) removeJob(source.id);
  const job = { sourceId: source.id, timer: null, stopped: false };
  jobs.set(source.id, job);
  scheduleNext(source, job);
}

function removeJob(id) {
  const task = jobs.get(id);
  if (task) {
    task.stopped = true;
    if (task.timer) clearTimeout(task.timer);
    jobs.delete(id);
  }
}

function restartJob(source) {
  removeJob(source.id);
  if (source.active) addJob(source);
}

function initScheduler() {
  const db = getDb();
  const sources = db.prepare('SELECT * FROM sources WHERE active = 1').all();
  for (const source of sources) {
    addJob(source);
  }
  console.log(`[scheduler] Initialized with ${sources.length} active source(s)`);
}

module.exports = {
  initScheduler,
  addJob,
  removeJob,
  restartJob,
  getEffectiveIntervalMinutes,
  getNextRunAt,
  getNextRunDelayMs,
};
