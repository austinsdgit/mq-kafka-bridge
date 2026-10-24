import { useCallback, useEffect, useRef, useState } from 'react'

// ── Types ──────────────────────────────────────────────────────────────────────
interface MessageResult {
  msg_id: string
  correl_id: string
  queue: string
  qmgr: string
  ts: string
}

interface ThrottleConfig {
  enabled: boolean
  rate_per_min: number   // 0 = unlimited
  updated_at: number
}

// ── API helpers ────────────────────────────────────────────────────────────────
async function apiPost(path: string, body: unknown) {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail ?? res.statusText)
  }
  return res.json()
}

async function apiPut(path: string, body: unknown) {
  const res = await fetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail ?? res.statusText)
  }
  return res.json()
}

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path)
  if (!res.ok) throw new Error(res.statusText)
  return res.json()
}

// ── MessageComposer ────────────────────────────────────────────────────────────
const BATCH_SIZES = [1, 10, 100, 1000]

/** Generate a 20-byte hex session prefix from crypto random bytes. */
function sessionPrefix(): string {
  const bytes = new Uint8Array(20)
  crypto.getRandomValues(bytes)
  return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
}

/** Encode a 32-bit counter as 8 hex chars (4 bytes big-endian). */
function counterHex(n: number): string {
  return (n >>> 0).toString(16).padStart(8, '0')
}

function MessageComposer({ onSent }: { onSent: (r: MessageResult) => void }) {
  const [payload, setPayload] = useState('')
  const [format, setFormat] = useState('MQSTR')
  const [persistent, setPersistent] = useState(true)
  const [count, setCount] = useState(1)
  const [progress, setProgress] = useState<{ sent: number; total: number } | null>(null)
  const [notice, setNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)

  // Stable 20-byte prefix for this component lifetime; counter increments per message
  const prefixRef = useRef(sessionPrefix())
  const counterRef = useRef(0)

  const loading = progress !== null

  function nextCorrelId(): string {
    const id = prefixRef.current + counterHex(counterRef.current)
    counterRef.current += 1
    return id
  }

  async function handleSend() {
    if (!payload.trim()) return
    setNotice(null)
    setProgress({ sent: 0, total: count })

    let succeeded = 0
    let lastEntry: MessageResult | null = null
    try {
      for (let i = 0; i < count; i++) {
        const result = await apiPost('/messages', {
          payload,
          correl_id: nextCorrelId(),
          format,
          persistence: persistent ? 1 : 0,
        })
        lastEntry = { ...result, ts: new Date().toISOString() }
        onSent(lastEntry as MessageResult)
        succeeded++
        setProgress({ sent: succeeded, total: count })
      }
      setNotice({
        kind: 'success',
        text: count === 1
          ? `Sent ✓  CorrelId: ${lastEntry!.correl_id.substring(0, 16)}…`
          : `Sent ${succeeded} of ${count} messages ✓`,
      })
      setPayload('')
    } catch (e: unknown) {
      setNotice({
        kind: 'error',
        text: count === 1
          ? String(e)
          : `Failed after ${succeeded}/${count} — ${String(e)}`,
      })
    } finally {
      setProgress(null)
    }
  }

  return (
    <section className="card">
      <h2>Send Message → PROXYQM INPUT.ALIAS</h2>

      <div style={{ marginBottom: 14 }}>
        <label htmlFor="payload">Message payload</label>
        <textarea
          id="payload"
          value={payload}
          onChange={e => setPayload(e.target.value)}
          placeholder='{"orderId": "ORD-001", "amount": 99.99}'
        />
      </div>

      <div className="row" style={{ marginBottom: 14 }}>
        <div>
          <label htmlFor="format">Format</label>
          <input
            id="format"
            type="text"
            value={format}
            onChange={e => setFormat(e.target.value)}
            maxLength={8}
          />
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, paddingBottom: 2 }}>
          <label style={{ marginBottom: 0 }}>
            <input
              type="checkbox"
              checked={persistent}
              onChange={e => setPersistent(e.target.checked)}
              style={{ marginRight: 6 }}
            />
            Persistent
          </label>
        </div>
      </div>

      <div className="row" style={{ alignItems: 'center', marginBottom: 14, gap: 8 }}>
        <label style={{ marginBottom: 0, whiteSpace: 'nowrap' }}>Send count</label>
        {BATCH_SIZES.map(n => (
          <button
            key={n}
            className={count === n ? 'primary' : 'ghost'}
            onClick={() => setCount(n)}
            disabled={loading}
            style={{ minWidth: 56 }}
          >
            {n}
          </button>
        ))}
      </div>

      <button
        className="primary"
        onClick={handleSend}
        disabled={loading || !payload.trim()}
      >
        {loading
          ? `Sending… ${progress!.sent}/${progress!.total}`
          : count === 1 ? 'Send to MQ' : `Send ${count}× to MQ`}
      </button>

      {notice && (
        <div className={`notice ${notice.kind}`}>{notice.text}</div>
      )}
    </section>
  )
}

// ── SentHistory ────────────────────────────────────────────────────────────────
function SentHistory({ items }: { items: MessageResult[] }) {
  if (items.length === 0) return null
  return (
    <section className="card">
      <h2>Sent messages</h2>
      <ul className="history-list">
        {[...items].reverse().map((r, i) => (
          <li key={i} className="history-item">
            <span className="ts">{new Date(r.ts).toLocaleTimeString()}</span>
            <span className="ids">
              MsgId <strong>{r.msg_id}</strong><br />
              CorrelId <strong>{r.correl_id}</strong>
            </span>
          </li>
        ))}
      </ul>
    </section>
  )
}

// ── QueueDepth ─────────────────────────────────────────────────────────────────
interface BothDepths {
  qmgra: { queue: string; qmgr: string; depth: number }
  qmgrb: { queue: string; qmgr: string; depth: number }
}

function QueueDepth() {
  const [both, setBoth] = useState<BothDepths | null>(null)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const refresh = useCallback(async () => {
    try {
      const data = await apiGet<BothDepths>('/queue/depth')
      setBoth(data)
      setError(null)
    } catch (e: unknown) {
      setError(String(e))
    }
  }, [])

  useEffect(() => {
    refresh()
    timerRef.current = setInterval(refresh, 10_000)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [refresh])

  return (
    <section className="card" style={{ minWidth: 180 }}>
      <h2>Queue depths</h2>
      {error
        ? <div className="notice error">{error}</div>
        : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {(['qmgra', 'qmgrb'] as const).map(key => (
              <div key={key}>
                <div style={{ fontSize: '.75rem', color: 'var(--muted)', marginBottom: 2 }}>
                  {key.toUpperCase()} · ORDERS.INBOUND
                </div>
                <div className="depth-display">
                  {both ? both[key].depth : '—'}
                </div>
              </div>
            ))}
            <div className="depth-label">refreshes every 10 s</div>
          </div>
        )
      }
    </section>
  )
}

// ── ThrottlePanel ──────────────────────────────────────────────────────────────
function ThrottlePanel() {
  const [cfg, setCfg] = useState<ThrottleConfig>({ enabled: true, rate_per_min: 0, updated_at: 0 })
  const [pendingRate, setPendingRate] = useState<string>('0')
  const [saving, setSaving] = useState(false)
  const [notice, setNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const refresh = useCallback(async () => {
    try {
      const data = await apiGet<ThrottleConfig>('/throttle')
      setCfg(data)
      // Only sync the input when we first load, not on every poll
      // (avoids overwriting user's in-progress edits)
    } catch {
      // silent — UI still functional
    }
  }, [])

  useEffect(() => {
    refresh().then(() => {
      setCfg(prev => { setPendingRate(String(prev.rate_per_min)); return prev })
    })
    timerRef.current = setInterval(refresh, 5_000)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [refresh])

  async function toggleEnabled() {
    setSaving(true)
    setNotice(null)
    try {
      const updated = await apiPut('/throttle', {
        enabled: !cfg.enabled,
        rate_per_min: cfg.rate_per_min,
      })
      setCfg(updated)
      setNotice({
        kind: 'success',
        text: updated.enabled ? 'Delete connector resumed' : 'Delete connector paused',
      })
    } catch (e: unknown) {
      setNotice({ kind: 'error', text: String(e) })
    } finally {
      setSaving(false)
    }
  }

  async function applyRate() {
    const rate = Math.max(0, Math.min(60, parseInt(pendingRate, 10) || 0))
    setSaving(true)
    setNotice(null)
    try {
      const updated = await apiPut('/throttle', { enabled: cfg.enabled, rate_per_min: rate })
      setCfg(updated)
      setPendingRate(String(updated.rate_per_min))
      setNotice({
        kind: 'success',
        text: rate === 0
          ? 'Rate set to unlimited'
          : `Rate set to ${rate} delete-cycle${rate === 1 ? '' : 's'}/min`,
      })
    } catch (e: unknown) {
      setNotice({ kind: 'error', text: String(e) })
    } finally {
      setSaving(false)
    }
  }

  const rateLabel = cfg.rate_per_min === 0 ? 'Unlimited' : `${cfg.rate_per_min}/min`

  return (
    <section className="card">
      <h2>Delete connector throttle</h2>
      <div className="throttle-controls">

        {/* Enable / pause toggle */}
        <div className="toggle-row">
          <label className="switch">
            <input
              type="checkbox"
              checked={cfg.enabled}
              onChange={toggleEnabled}
              disabled={saving}
            />
            <span className="slider" />
          </label>
          <span className="toggle-label">
            {cfg.enabled
              ? <span className="badge green">● Running</span>
              : <span className="badge red">■ Paused</span>}
          </span>
          <span style={{ color: 'var(--muted)', fontSize: '.8rem' }}>
            Current rate: <strong>{rateLabel}</strong>
          </span>
        </div>

        <hr className="divider" />

        {/* Rate control */}
        <div>
          <label htmlFor="rateInput">
            Max delete-cycles per minute &nbsp;
            <span style={{ fontWeight: 400, color: 'var(--muted)' }}>(0 = unlimited, max 60)</span>
          </label>
          <div className="row" style={{ marginTop: 6 }}>
            <input
              id="rateInput"
              type="number"
              min={0}
              max={60}
              value={pendingRate}
              onChange={e => setPendingRate(e.target.value)}
              style={{ maxWidth: 120 }}
            />
            <button
              className="primary"
              onClick={applyRate}
              disabled={saving}
              style={{ flex: 'none' }}
            >
              Apply rate
            </button>
            <button
              className="ghost"
              onClick={async () => {
                setPendingRate('0')
                setSaving(true)
                setNotice(null)
                try {
                  const updated = await apiPut('/throttle', { enabled: cfg.enabled, rate_per_min: 0 })
                  setCfg(updated)
                  setNotice({ kind: 'success', text: 'Rate set to unlimited' })
                } catch (e: unknown) {
                  setNotice({ kind: 'error', text: String(e) })
                } finally {
                  setSaving(false)
                }
              }}
              disabled={saving}
              style={{ flex: 'none' }}
            >
              Unlimited
            </button>
          </div>
          <div style={{ marginTop: 8, fontSize: '.8rem', color: 'var(--muted)' }}>
            Delay between cycles: &nbsp;
            <strong>
              {cfg.rate_per_min === 0 ? 'none' : `${(60 / cfg.rate_per_min).toFixed(1)} s`}
            </strong>
          </div>
        </div>

        {notice && (
          <div className={`notice ${notice.kind}`}>{notice.text}</div>
        )}
      </div>
    </section>
  )
}

// ── ConsumePanel ───────────────────────────────────────────────────────────────
function ConsumePanel() {
  const [loading, setLoading] = useState(false)
  const [notice, setNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)
  const [drainArmed, setDrainArmed] = useState(false)
  const [draining, setDraining] = useState(false)
  const armTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  async function handleConsume() {
    setLoading(true)
    setNotice(null)
    try {
      const result = await apiPost('/queue/a/consume', {})
      setNotice({
        kind: 'success',
        text: result.consumed === 0
          ? 'QMGRA queue was already empty'
          : `Consumed ${result.consumed} message${result.consumed === 1 ? '' : 's'} from ${result.qmgr} ${result.queue}`,
      })
    } catch (e: unknown) {
      setNotice({ kind: 'error', text: String(e) })
    } finally {
      setLoading(false)
    }
  }

  function armDrain() {
    setDrainArmed(true)
    setNotice(null)
    // Auto-disarm after 5 s if user doesn't confirm
    if (armTimerRef.current) clearTimeout(armTimerRef.current)
    armTimerRef.current = setTimeout(() => setDrainArmed(false), 5000)
  }

  async function confirmDrain() {
    if (armTimerRef.current) clearTimeout(armTimerRef.current)
    setDrainArmed(false)
    setDraining(true)
    setNotice(null)
    try {
      const result = await apiPost('/queue/b/consume', {})
      setNotice({
        kind: 'success',
        text: result.consumed === 0
          ? 'QMGRB queue was already empty'
          : `Drained ${result.consumed} message${result.consumed === 1 ? '' : 's'} from ${result.qmgr} ${result.queue}`,
      })
    } catch (e: unknown) {
      setNotice({ kind: 'error', text: String(e) })
    } finally {
      setDraining(false)
    }
  }

  return (
    <section className="card">
      <h2>Queue controls</h2>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <div>
          <p style={{ fontSize: '.85rem', color: 'var(--muted)', margin: '0 0 8px' }}>
            Consume all messages from <strong>ORDERS.INBOUND</strong> on QMGRA.
          </p>
          <button className="primary" onClick={handleConsume} disabled={loading || draining}>
            {loading ? 'Consuming…' : 'Consume QMGRA'}
          </button>
        </div>
        <hr style={{ border: 'none', borderTop: '1px solid var(--border)', margin: 0 }} />
        <div>
          <p style={{ fontSize: '.85rem', color: 'var(--muted)', margin: '0 0 8px' }}>
            Drain all messages from <strong>ORDERS.INBOUND</strong> on QMGRB.
            Click once to arm, then confirm within 5 s.
          </p>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {!drainArmed
              ? <button className="ghost" onClick={armDrain} disabled={loading || draining}
                  style={{ borderColor: '#dc2626', color: '#dc2626' }}>
                  {draining ? 'Draining…' : 'Drain QMGRB'}
                </button>
              : <button className="primary" onClick={confirmDrain}
                  style={{ background: '#dc2626', borderColor: '#dc2626' }}>
                  ⚠ Confirm drain QMGRB
                </button>
            }
            {drainArmed && (
              <span style={{ fontSize: '.8rem', color: '#dc2626' }}>Armed — confirm or wait 5 s</span>
            )}
          </div>
        </div>
      </div>
      {notice && <div className={`notice ${notice.kind}`} style={{ marginTop: 12 }}>{notice.text}</div>}
    </section>
  )
}

// ── DeleteEventFeed ────────────────────────────────────────────────────────────
interface DeleteEvent {
  correl_id: string
  partition: number
  offset: number
  deleted: number
  depth: number
  timestamp_utc: string
}

function DeleteEventFeed() {
  const [events, setEvents] = useState<DeleteEvent[]>([])
  const [connected, setConnected] = useState(false)
  const listRef = useRef<HTMLUListElement>(null)

  useEffect(() => {
    const es = new EventSource('/events/delete')

    es.addEventListener('connected', () => setConnected(true))

    es.addEventListener('delete', (e: MessageEvent) => {
      try {
        const evt: DeleteEvent = JSON.parse(e.data)
        setEvents(prev => [evt, ...prev].slice(0, 50))
      } catch {
        // ignore malformed event
      }
    })

    es.onerror = () => setConnected(false)

    return () => es.close()
  }, [])

  return (
    <section className="card">
      <h2 style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        QMGRB delete connector
        <span className={`badge ${connected ? 'green' : 'red'}`} style={{ fontSize: '.7rem' }}>
          {connected ? '● Live' : '○ Disconnected'}
        </span>
      </h2>
      <p style={{ fontSize: '.85rem', color: 'var(--muted)', marginBottom: 12 }}>
        Events fired when the delete connector processes a Kafka message and removes from <strong>ORDERS.INBOUND</strong> on QMGRB.
      </p>
      {events.length === 0
        ? <div style={{ fontSize: '.85rem', color: 'var(--muted)' }}>Waiting for events…</div>
        : (
          <ul ref={listRef} className="history-list" style={{ maxHeight: 260, overflowY: 'auto' }}>
            {events.map((evt, i) => (
              <li key={i} className="history-item">
                <span className="ts">{new Date(evt.timestamp_utc).toLocaleTimeString()}</span>
                <span className="ids">
                  Kafka partition={evt.partition} offset={evt.offset}
                  <br />
                  depth signal <strong>{evt.depth}</strong> &nbsp;·&nbsp; deleted <strong>{evt.deleted}</strong>
                </span>
              </li>
            ))}
          </ul>
        )
      }
    </section>
  )
}

// ── App ────────────────────────────────────────────────────────────────────────
export default function App() {
  const [history, setHistory] = useState<MessageResult[]>([])

  return (
    <div className="layout">
      <header>
        <h1>MQ-Kafka Bridge</h1>
        <span className="badge blue">PROXYQM → QMGRA / QMGRB → Kafka</span>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 24, alignItems: 'start' }}>
        <MessageComposer onSent={r => setHistory(h => [...h, r])} />
        <QueueDepth />
      </div>

      <SentHistory items={history} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        <ConsumePanel />
        <DeleteEventFeed />
      </div>

      <ThrottlePanel />
    </div>
  )
}
