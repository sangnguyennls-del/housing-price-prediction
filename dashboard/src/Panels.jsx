import { useEffect, useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { api } from './api'

const fmt = (x, d = 1) => (x == null ? '—' : Number(x).toLocaleString('vi-VN',
  { minimumFractionDigits: d, maximumFractionDigits: d }))

function useAsync(fn, deps = []) {
  const [state, setState] = useState({ loading: true, data: null, error: null })
  useEffect(() => {
    let alive = true
    setState({ loading: true, data: null, error: null })
    fn().then((d) => alive && setState({ loading: false, data: d, error: null }))
        .catch((e) => alive && setState({ loading: false, data: null, error: e.message }))
    return () => { alive = false }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)
  return state
}

function Block({ state, children, empty = 'Chưa có dữ liệu.' }) {
  if (state.loading) return <div className="muted">Đang tải…</div>
  if (state.error) return <div className="err">{state.error}</div>
  if (!state.data || (Array.isArray(state.data) && !state.data.length))
    return <div className="muted">{empty}</div>
  return children(state.data)
}

// ══════════════════════════════════════════════════════════════════
// Bài toán 1 — định giá
// ══════════════════════════════════════════════════════════════════
const EMPTY = {
  address: 'Quận 7, TP. Hồ Chí Minh', area: 80, bedrooms: 3, bathrooms: 2,
  floors: 3, frontage: 5, access_road: 6, legal_status: 'Have certificate',
  furniture_state: 'Basic', house_direction: 'Đông',
}

export function PredictPanel() {
  const [form, setForm] = useState(EMPTY)
  const [res, setRes] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)

  const set = (k) => (e) => setForm({ ...form, [k]: e.target.value })

  const submit = async (e) => {
    e.preventDefault()
    setBusy(true); setErr(null); setRes(null)
    try {
      const body = { ...form }
      // Ô trống phải gửi đi là null, KHÔNG phải chuỗi rỗng hay 0: mô hình cây
      // xử lý giá trị khuyết được, còn 0 thì nó hiểu là "nhà 0 tầng".
      ;['area', 'bedrooms', 'bathrooms', 'floors', 'frontage', 'access_road']
        .forEach((k) => { body[k] = body[k] === '' || body[k] == null ? null : Number(body[k]) })
      setRes(await api.predict(body))
    } catch (e2) { setErr(e2.message) } finally { setBusy(false) }
  }

  return (
    <div className="grid2">
      <form className="card" onSubmit={submit}>
        <h3>Nhập thông tin bất động sản</h3>
        <label>Địa chỉ<input value={form.address} onChange={set('address')}
          placeholder="123 Nguyễn Thị Thập, Quận 7, TP.HCM" /></label>
        <div className="row">
          <label>Diện tích (m²)<input type="number" value={form.area} onChange={set('area')} required /></label>
          <label>Số tầng<input type="number" value={form.floors} onChange={set('floors')} /></label>
        </div>
        <div className="row">
          <label>Phòng ngủ<input type="number" value={form.bedrooms} onChange={set('bedrooms')} /></label>
          <label>Phòng tắm<input type="number" value={form.bathrooms} onChange={set('bathrooms')} /></label>
        </div>
        <div className="row">
          <label>Mặt tiền (m)<input type="number" step="0.1" value={form.frontage} onChange={set('frontage')} /></label>
          <label>Đường vào (m)<input type="number" step="0.1" value={form.access_road} onChange={set('access_road')} /></label>
        </div>
        <div className="row">
          <label>Pháp lý
            <select value={form.legal_status} onChange={set('legal_status')}>
              <option value="Have certificate">Đã có sổ</option>
              <option value="Sale contract">Hợp đồng mua bán</option>
              <option value="Waiting for certificate">Đang chờ sổ</option>
              <option value="">Không rõ</option>
            </select>
          </label>
          <label>Hướng nhà
            <select value={form.house_direction} onChange={set('house_direction')}>
              {['', 'Đông', 'Tây', 'Nam', 'Bắc', 'Đông Bắc', 'Đông Nam', 'Tây Nam', 'Tây Bắc']
                .map((d) => <option key={d} value={d}>{d || 'Không rõ'}</option>)}
            </select>
          </label>
        </div>
        <button disabled={busy}>{busy ? 'Đang tính…' : 'Định giá'}</button>
        {err && <div className="err">{err}</div>}
      </form>

      <div className="card">
        <h3>Kết quả</h3>
        {!res && <div className="muted">Điền thông tin rồi bấm “Định giá”.</div>}
        {res && (
          <>
            <div className="bignum">{fmt(res.price_ty, 2)} <small>tỷ VND</small></div>
            <div className="muted">
              {fmt(res.price_per_m2)} triệu/m² · khoảng {fmt(res.interval_ty[0], 2)}–
              {fmt(res.interval_ty[1], 2)} tỷ
            </div>
            <table className="kv">
              <tbody>
                <tr><td>Khu vực nhận dạng</td><td>{res.geo.district || '—'}, {res.geo.province || '—'}
                  <span className="tag">{res.geo.source} · {res.geo.match_score}</span></td></tr>
                <tr><td>Trung vị quận</td><td>{fmt(res.district_median_m2)} triệu/m²</td></tr>
                <tr><td>So với mặt bằng quận</td>
                    <td className={res.vs_district_pct >= 0 ? 'up' : 'down'}>
                      {res.vs_district_pct >= 0 ? '+' : ''}{fmt(res.vs_district_pct)}%</td></tr>
                <tr><td>Độ trễ suy luận</td><td>{fmt(res.latency_ms, 1)} ms</td></tr>
              </tbody>
            </table>
            <p className="note">{res.note}</p>
            <p className="note">
              Khoảng giá suy từ MAPE đo trên tập kiểm tra (19,6%), không phải khoảng
              tin cậy thống kê — cây tăng cường không cho đại lượng đó.
            </p>
          </>
        )}
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════
// Bài toán 4 — phân cụm
// ══════════════════════════════════════════════════════════════════
const CLUSTER_COLORS = ['#d62728', '#1f77b4', '#2ca02c', '#ff7f0e', '#9467bd']

export function ClusterPanel() {
  const st = useAsync(() => api.clusters(), [])
  return (
    <Block state={st} empty="Chưa chạy ml/train_cluster.py.">
      {(rows) => {
        const groups = [...new Set(rows.map((r) => r.cluster_id))].sort()
        const option = {
          tooltip: {
            formatter: (p) => `<b>${p.data[2]}</b><br/>${p.data[3]}<br/>` +
              `Trung vị: ${fmt(p.data[4])} triệu/m²`,
          },
          legend: { data: groups.map((g) => rows.find((r) => r.cluster_id === g).cluster_label) },
          grid: { left: 50, right: 20, top: 40, bottom: 40 },
          xAxis: { name: 'PCA 1', type: 'value', scale: true },
          yAxis: { name: 'PCA 2', type: 'value', scale: true },
          series: groups.map((g, i) => {
            const sub = rows.filter((r) => r.cluster_id === g)
            return {
              name: sub[0].cluster_label,
              type: 'scatter',
              symbolSize: 12,
              itemStyle: { color: CLUSTER_COLORS[i % CLUSTER_COLORS.length] },
              data: sub.map((r) => [r.pca_x, r.pca_y, r.area_name,
                                    r.province_name, r.median_price_m2]),
            }
          }),
        }
        return (
          <div className="grid2">
            <div className="card">
              <h3>Không gian phân cụm (PCA 2 chiều)</h3>
              <ReactECharts option={option} style={{ height: 420 }} />
              <p className="note">
                Mỗi điểm là một quận/huyện. Trục là hai thành phần chính của
                vector đặc trưng — không phải toạ độ địa lý.
              </p>
            </div>
            <div className="card scroll">
              <h3>Danh sách theo cụm</h3>
              <table className="tbl">
                <thead><tr><th>Quận/huyện</th><th>Tỉnh/TP</th><th>Cụm</th>
                  <th className="r">Trung vị</th></tr></thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.area_code}>
                      <td>{r.area_name}</td>
                      <td className="muted">{r.province_name}</td>
                      <td><span className="pill" style={{
                        background: CLUSTER_COLORS[groups.indexOf(r.cluster_id) % CLUSTER_COLORS.length],
                      }}>{r.cluster_label}</span></td>
                      <td className="r">{fmt(r.median_price_m2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )
      }}
    </Block>
  )
}

// ══════════════════════════════════════════════════════════════════
// Bài toán 5 — bất thường
// ══════════════════════════════════════════════════════════════════
export function AnomalyPanel() {
  const st = useAsync(() => api.anomalies(100), [])
  return (
    <Block state={st} empty="Chưa chạy ml/train_anomaly.py.">
      {(rows) => (
        <div className="card scroll">
          <h3>{rows.length} tin bị gắn cờ, xếp theo điểm bất thường</h3>
          <p className="note">
            Điểm dựa trên DƯ của mô hình giá: tin lệch xa mức mô hình dự đoán cho
            cùng vị trí và cùng thuộc tính. Đây là nghi vấn cần người kiểm chứng,
            không phải kết luận tin giả.
          </p>
          <table className="tbl">
            <thead><tr><th>Quận/huyện</th><th className="r">Giá rao</th>
              <th className="r">Đơn giá</th><th className="r">Mô hình</th>
              <th className="r">Lệch</th><th className="r">Điểm</th><th>Diễn giải</th></tr></thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.listing_id}>
                  <td>{r.district}</td>
                  <td className="r">{fmt(r.price, 2)} tỷ</td>
                  <td className="r">{fmt(r.price_per_m2)}</td>
                  <td className="r">{fmt(r.predicted_price_m2)}</td>
                  <td className={`r ${r.price_per_m2 > r.predicted_price_m2 ? 'up' : 'down'}`}>
                    {fmt(r.residual_ratio * 100, 0)}%</td>
                  <td className="r"><b>{fmt(r.anomaly_score, 0)}</b></td>
                  <td className="muted">{r.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

// ══════════════════════════════════════════════════════════════════
// Bài toán 3 — dự báo
// ══════════════════════════════════════════════════════════════════
export function ForecastPanel() {
  const st = useAsync(() => api.forecast(), [])
  return (
    <Block state={st}>
      {(d) => {
        if (!d.history?.length) {
          return (
            <div className="card">
              <h3>Chưa có chuỗi thời gian</h3>
              <p className="note">{d.note || 'Chạy scripts/build_price_history.py.'}</p>
              <p className="note">
                Dataset tin rao chính không có cột ngày. Chuỗi thời gian phải đến
                từ ngày đăng tin do crawler thu thập, hoặc từ nguồn chỉ số giá
                công bố công khai — không được rải ngẫu nhiên tin lên trục thời gian.
              </p>
            </div>
          )
        }
        const models = [...new Set(d.forecast.map((f) => f.model_name))]
        const option = {
          tooltip: { trigger: 'axis' },
          legend: { data: ['Thực tế', ...models] },
          grid: { left: 60, right: 20, top: 40, bottom: 50 },
          xAxis: { type: 'time' },
          yAxis: { type: 'value', name: 'triệu/m²', scale: true },
          series: [
            { name: 'Thực tế', type: 'line', showSymbol: true, symbolSize: 5,
              lineStyle: { width: 2, color: '#333' }, itemStyle: { color: '#333' },
              data: d.history.map((h) => [h.ds, h.price_m2]) },
            ...models.map((m, i) => ({
              name: m, type: 'line', showSymbol: true,
              lineStyle: { type: 'dashed', color: CLUSTER_COLORS[i] },
              itemStyle: { color: CLUSTER_COLORS[i] },
              data: d.forecast.filter((f) => f.model_name === m)
                .map((f) => [f.ds, f.yhat]),
            })),
          ],
        }
        return (
          <div className="grid2">
            <div className="card">
              <h3>Chuỗi giá và dự báo — {d.areaCode}</h3>
              <ReactECharts option={option} style={{ height: 400 }} />
            </div>
            <div className="card">
              <h3>Backtest cuốn chiếu</h3>
              <table className="tbl">
                <thead><tr><th>Mô hình</th><th className="r">MAPE %</th>
                  <th className="r">RMSE</th><th className="r">MAE</th><th className="r">Fold</th></tr></thead>
                <tbody>
                  {d.metrics.map((m) => (
                    <tr key={m.model_name}>
                      <td>{m.model_name}</td>
                      <td className="r"><b>{fmt(m.mape, 2)}</b></td>
                      <td className="r">{fmt(m.rmse, 2)}</td>
                      <td className="r">{fmt(m.mae, 2)}</td>
                      <td className="r">{m.n_folds}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="note">
                Mô hình <b>naive</b> (lặp lại giá trị cuối) là mốc đối chứng bắt
                buộc. Prophet/LSTM chỉ có giá trị khi thắng được nó.
              </p>
            </div>
          </div>
        )
      }}
    </Block>
  )
}

// ══════════════════════════════════════════════════════════════════
// Hot path
// ══════════════════════════════════════════════════════════════════
export function HotPanel() {
  const [tick, setTick] = useState(0)
  useEffect(() => {
    // Tự làm mới 10 giây/lần — bằng đúng nhịp micro-batch của Spark Streaming.
    // Làm mới nhanh hơn chỉ tốn request mà không có dữ liệu mới.
    const id = setInterval(() => setTick((t) => t + 1), 10_000)
    return () => clearInterval(id)
  }, [])
  const vel = useAsync(() => api.hotVelocity(15), [tick])
  const alerts = useAsync(() => api.hotAlerts(30), [tick])

  return (
    <div className="grid2">
      <div className="card">
        <h3>Vận tốc đăng tin — cửa sổ trượt 1 giờ</h3>
        <Block state={vel} empty="Redis chưa có dữ liệu — chạy spark/streaming_hot_path.py.">
          {(rows) => (
            <table className="tbl">
              <thead><tr><th>Quận/huyện</th><th className="r">Tin mới</th>
                <th className="r">Đơn giá TB</th><th>Cửa sổ kết thúc</th></tr></thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.districtCode}>
                    <td>{r.district || r.districtCode}</td>
                    <td className="r"><b>{r.nListings}</b></td>
                    <td className="r">{fmt(r.avg_price_m2)}</td>
                    <td className="muted">{r.win_end?.slice(0, 16)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Block>
        <p className="note">
          Hot path dùng TRUNG BÌNH, khác cold path dùng trung vị: Spark không
          tính được percentile trên trạng thái streaming cập nhật tăng dần.
        </p>
      </div>

      <div className="card scroll">
        <h3>Cảnh báo tin lệch giá (thời gian thực)</h3>
        <Block state={alerts} empty="Chưa có cảnh báo nào.">
          {(d) => (
            <>
              <div className="muted">Tổng cộng đã phát: {d.total}</div>
              <table className="tbl">
                <thead><tr><th>Quận/huyện</th><th className="r">Giá rao</th>
                  <th className="r">Mặt bằng</th><th className="r">Lệch</th><th>Lý do</th></tr></thead>
                <tbody>
                  {(d.alerts || []).map((raw, i) => {
                    const a = typeof raw === 'string' ? JSON.parse(raw) : raw
                    return (
                      <tr key={a.listing_id || i}>
                        <td>{a.district}</td>
                        <td className="r">{fmt(a.price, 2)} tỷ</td>
                        <td className="r">{fmt(a.base_price_m2)}</td>
                        <td className={`r ${a.deviation > 0 ? 'up' : 'down'}`}>
                          {fmt(a.deviation * 100, 0)}%</td>
                        <td className="muted">{a.reason}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </>
          )}
        </Block>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════
// Chất lượng dữ liệu — bằng chứng cho báo cáo
// ══════════════════════════════════════════════════════════════════
export function QualityPanel() {
  const st = useAsync(() => api.quality(), [])
  return (
    <Block state={st} empty="Chưa có lần chạy ETL nào.">
      {(rows) => (
        <div className="card scroll">
          <h3>Chất lượng chuẩn hóa qua từng lần chạy ETL</h3>
          <p className="note">
            Tỷ lệ khớp địa chỉ là chỉ số quyết định của cả hệ thống: vị trí là
            đặc trưng mạnh nhất của bài toán giá nhà, và heatmap không vẽ được
            nếu không quy được địa chỉ về mã hành chính.
          </p>
          <table className="tbl">
            <thead><tr><th>Thời điểm</th><th>Giai đoạn</th><th className="r">Vào</th>
              <th className="r">Ra</th><th className="r">Loại</th><th className="r">Tỉnh</th>
              <th className="r">Quận</th><th className="r">Phường</th><th>Ghi chú</th></tr></thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  <td className="muted">{String(r.run_at).slice(0, 19).replace('T', ' ')}</td>
                  <td>{r.stage}</td>
                  <td className="r">{r.rows_in?.toLocaleString('vi-VN')}</td>
                  <td className="r">{r.rows_out?.toLocaleString('vi-VN')}</td>
                  <td className="r">{r.rows_dropped?.toLocaleString('vi-VN')}</td>
                  <td className="r">{r.province_match_rate != null ? `${fmt(r.province_match_rate * 100)}%` : '—'}</td>
                  <td className="r"><b>{r.district_match_rate != null ? `${fmt(r.district_match_rate * 100)}%` : '—'}</b></td>
                  <td className="r">{r.ward_match_rate != null ? `${fmt(r.ward_match_rate * 100)}%` : '—'}</td>
                  <td className="muted">{r.notes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

export { useAsync, Block, fmt }
