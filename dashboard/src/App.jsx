import { useState } from 'react'
import MapView from './MapView'
import {
  AnomalyPanel, Block, ClusterPanel, ForecastPanel, HotPanel,
  PredictPanel, QualityPanel, fmt, useAsync,
} from './Panels'
import { api } from './api'

const TABS = [
  { id: 'map', label: 'Bản đồ giá', hint: 'Bài toán 2' },
  { id: 'predict', label: 'Định giá', hint: 'Bài toán 1' },
  { id: 'forecast', label: 'Dự báo xu hướng', hint: 'Bài toán 3' },
  { id: 'cluster', label: 'Phân cụm khu vực', hint: 'Bài toán 4' },
  { id: 'anomaly', label: 'Tin bất thường', hint: 'Bài toán 5' },
  { id: 'hot', label: 'Thời gian thực', hint: 'Hot path' },
  { id: 'quality', label: 'Chất lượng dữ liệu', hint: 'ETL' },
]

const KPI_ORDER = ['total_listings', 'median_price_m2', 'median_price',
                   'median_area', 'n_districts', 'n_provinces']
const KPI_LABEL = {
  total_listings: 'Tin rao đã xử lý',
  median_price_m2: 'Trung vị đơn giá',
  median_price: 'Trung vị giá',
  median_area: 'Trung vị diện tích',
  n_districts: 'Quận/huyện có dữ liệu',
  n_provinces: 'Tỉnh/thành có dữ liệu',
}

function KpiBar() {
  const st = useAsync(() => api.overview(), [])
  return (
    <Block state={st} empty="Chưa có dữ liệu Gold.">
      {(d) => (
        <div className="kpis">
          {KPI_ORDER.filter((k) => d.summary[k]).map((k) => (
            <div className="kpi" key={k}>
              <div className="kpi-val">
                {k === 'total_listings'
                  ? Number(d.summary[k].value).toLocaleString('vi-VN')
                  : fmt(d.summary[k].value, k.startsWith('n_') ? 0 : 1)}
              </div>
              <div className="kpi-lbl">{KPI_LABEL[k]}</div>
              <div className="kpi-unit">{d.summary[k].text}</div>
            </div>
          ))}
          <div className={`kpi ${d.hot?.available ? 'live' : 'off'}`}>
            <div className="kpi-val">{d.hot?.available ? d.hot.velocity.length : '—'}</div>
            <div className="kpi-lbl">Hot path</div>
            <div className="kpi-unit">
              {d.hot?.available
                ? `${d.hot.alertTotal} cảnh báo đã phát`
                : (d.hot?.reason || 'chưa chạy')}
            </div>
          </div>
        </div>
      )}
    </Block>
  )
}

export default function App() {
  const [tab, setTab] = useState('map')
  const [level, setLevel] = useState('district')

  return (
    <div className="app">
      <header>
        <div>
          <h1>Bản đồ giá bất động sản Việt Nam</h1>
          <div className="sub">
            IE221 — Công nghệ Dữ liệu lớn · Kiến trúc Lambda: Kafka → Spark →
            HDFS/PostgreSQL/Redis → MLflow
          </div>
        </div>
      </header>

      <KpiBar />

      <nav className="tabs">
        {TABS.map((t) => (
          <button key={t.id} className={t.id === tab ? 'on' : ''} onClick={() => setTab(t.id)}>
            {t.label}<span className="hint">{t.hint}</span>
          </button>
        ))}
      </nav>

      <main>
        {tab === 'map' && <MapView level={level} onLevelChange={setLevel} />}
        {tab === 'predict' && <PredictPanel />}
        {tab === 'forecast' && <ForecastPanel />}
        {tab === 'cluster' && <ClusterPanel />}
        {tab === 'anomaly' && <AnomalyPanel />}
        {tab === 'hot' && <HotPanel />}
        {tab === 'quality' && <QualityPanel />}
      </main>

      <footer>
        Dữ liệu: tin RAO công khai (batdongsan.com.vn qua Kaggle + alonhadat.com.vn).
        Giá rao khác giá giao dịch — mọi con số ở đây là ước lượng thị trường chào bán,
        không phải định giá pháp lý.
      </footer>
    </div>
  )
}
