import { useEffect, useMemo, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet'
import { api } from './api'

// Thang màu tuần tự (YlOrRd). Dùng thang TUẦN TỰ chứ không phải thang phân kỳ:
// giá là đại lượng một chiều "thấp → cao", không có điểm giữa trung tính nào
// để phân kỳ quanh đó.
const COLORS = ['#ffffb2', '#fed976', '#feb24c', '#fd8d3c', '#f03b20', '#bd0026']

/**
 * Chia lớp theo PHÂN VỊ chứ không chia đều khoảng giá.
 *
 * Giá bất động sản lệch phải rất mạnh: chia đều khoảng sẽ dồn 95% quận vào
 * lớp màu nhạt nhất và để vài quận trung tâm chiếm hết dải màu — bản đồ trông
 * như cả nước đồng giá. Chia theo phân vị đảm bảo mỗi lớp có số quận tương
 * đương, nên khác biệt vùng hiện ra được.
 */
function quantileBreaks(values, n = COLORS.length) {
  const v = values.filter((x) => x != null && !Number.isNaN(x)).sort((a, b) => a - b)
  if (!v.length) return []
  return Array.from({ length: n - 1 }, (_, i) => v[Math.floor(((i + 1) / n) * (v.length - 1))])
}

function colorFor(value, breaks) {
  if (value == null) return '#e8e8e8' // không có dữ liệu — xám, không phải màu thấp nhất
  let i = 0
  while (i < breaks.length && value > breaks[i]) i += 1
  return COLORS[i]
}

const VN_CENTER = [16.0, 107.0]

export default function MapView({ level, onLevelChange }) {
  const [geo, setGeo] = useState(null)
  const [rows, setRows] = useState([])
  const [error, setError] = useState(null)
  const [hovered, setHovered] = useState(null)

  useEffect(() => {
    setGeo(null)
    setError(null)
    Promise.all([api.geo(level === 'province' ? 'provinces' : 'districts'),
                 api.areaPrices(level)])
      .then(([g, r]) => { setGeo(g); setRows(r) })
      .catch((e) => setError(e.message))
  }, [level])

  // Ghép GeoJSON với dữ liệu giá BẰNG MÃ HÀNH CHÍNH, không bằng tên. Tên tiếng
  // Việt có dấu, viết hoa/thường và tiền tố ("Quận 1" / "Q.1") khác nhau giữa
  // hai nguồn; ghép theo tên là cách chắc chắn nhất để bản đồ trống một nửa.
  const byCode = useMemo(() => {
    const m = new Map()
    rows.forEach((r) => m.set(r.area_code, r))
    return m
  }, [rows])

  const breaks = useMemo(
    () => quantileBreaks(rows.map((r) => r.median_price_m2)), [rows])

  const matched = useMemo(() => {
    if (!geo) return 0
    return geo.features.filter((f) => byCode.has(f.properties.code)).length
  }, [geo, byCode])

  const style = (feature) => {
    const row = byCode.get(feature.properties.code)
    return {
      fillColor: colorFor(row?.median_price_m2, breaks),
      weight: 0.5,
      color: '#666',
      fillOpacity: row ? 0.78 : 0.25,
    }
  }

  const onEach = (feature, layer) => {
    const row = byCode.get(feature.properties.code)
    const name = feature.properties.name
    layer.bindTooltip(
      row
        ? `<b>${name}</b><br/>Trung vị: <b>${row.median_price_m2.toFixed(1)}</b> triệu/m²` +
          `<br/>Khoảng giữa: ${row.p25_price_m2?.toFixed(0)}–${row.p75_price_m2?.toFixed(0)}` +
          `<br/>${row.n_listings.toLocaleString('vi-VN')} tin rao`
        : `<b>${name}</b><br/><i>Không đủ dữ liệu (< 30 tin)</i>`,
      { sticky: true })
    layer.on({
      mouseover: () => setHovered(row ? { ...row, name } : { name }),
      mouseout: () => setHovered(null),
    })
  }

  return (
    <div className="map-wrap">
      <div className="map-toolbar">
        <div className="seg">
          {['province', 'district'].map((lv) => (
            <button key={lv} className={lv === level ? 'on' : ''}
                    onClick={() => onLevelChange(lv)}>
              {lv === 'province' ? 'Cấp tỉnh/thành' : 'Cấp quận/huyện'}
            </button>
          ))}
        </div>
        <div className="legend">
          <span className="legend-label">Trung vị (triệu/m²)</span>
          {COLORS.map((c, i) => (
            <span key={c} className="legend-box" style={{ background: c }}>
              {i === 0 ? `<${breaks[0]?.toFixed(0) ?? ''}`
                       : i === COLORS.length - 1 ? `>${breaks[i - 1]?.toFixed(0) ?? ''}`
                       : `${breaks[i - 1]?.toFixed(0) ?? ''}`}
            </span>
          ))}
          <span className="legend-box" style={{ background: '#e8e8e8' }}>n/a</span>
        </div>
      </div>

      {error && <div className="err">Không tải được bản đồ: {error}</div>}

      <MapContainer center={VN_CENTER} zoom={6} className="map"
                    scrollWheelZoom preferCanvas>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
        {geo && <GeoJSON key={level} data={geo} style={style} onEachFeature={onEach} />}
      </MapContainer>

      <div className="map-foot">
        {geo
          ? <>Ghép được <b>{matched}</b>/{geo.features.length} đơn vị hành chính có dữ liệu.
              {' '}Đơn vị dưới 30 tin rao để xám — trung vị trên mẫu quá nhỏ sẽ gây hiểu sai.</>
          : 'Đang tải ranh giới hành chính…'}
        {hovered && <span className="hover"> · Đang xem: <b>{hovered.name}</b></span>}
      </div>
    </div>
  )
}
