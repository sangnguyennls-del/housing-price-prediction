// Mọi lời gọi mạng của dashboard đi qua đây, và mọi lời gọi đều tới GATEWAY.
// Frontend không biết PostgreSQL, Redis hay FastAPI tồn tại — đó là mục đích
// của tầng gateway, và cũng là lý do file này chỉ có một base URL duy nhất.
const BASE = '/api'

async function get(path) {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`)
  return r.json()
}

async function post(path, body) {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const data = await r.json()
  if (!r.ok) throw new Error(data.detail || data.error || `HTTP ${r.status}`)
  return data
}

export const api = {
  overview: () => get('/dashboard/overview'),
  areaPrices: (level) => get(`/areas/price?level=${level}`),
  clusters: () => get('/clusters'),
  anomalies: (n = 50) => get(`/anomalies?limit=${n}`),
  forecast: (code) => get(`/forecast${code ? `?areaCode=${code}` : ''}`),
  quality: () => get('/quality'),
  hotVelocity: (n = 20) => get(`/hot/velocity?limit=${n}`),
  hotAlerts: (n = 50) => get(`/hot/alerts?limit=${n}`),
  predict: (body) => post('/predict', body),
  anomalyCheck: (body) => post('/anomaly', body),
  // GeoJSON là file tĩnh do Vite phục vụ, không đi qua gateway: nó không đổi
  // theo dữ liệu và nặng ~1MB, cho trình duyệt cache thẳng là hợp lý nhất.
  geo: (level) => fetch(`/geo/${level}.geojson`).then((r) => r.json()),
}
