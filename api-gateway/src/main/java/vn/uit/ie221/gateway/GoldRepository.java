package vn.uit.ie221.gateway;

import java.util.List;
import java.util.Map;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Đọc tầng Gold trong PostgreSQL.
 *
 * <p>Cố ý dùng {@link JdbcTemplate} và trả {@code Map} thay vì JPA + entity:
 * gateway chỉ ĐỌC dữ liệu đã tổng hợp sẵn và chuyển thẳng ra JSON. Dựng một
 * tầng entity/repository đầy đủ cho việc đó là bộ khung không ai dùng tới —
 * mỗi cột thêm vào tầng Gold sẽ phải sửa ở hai nơi.
 */
@Repository
public class GoldRepository {

    private final JdbcTemplate jdbc;

    public GoldRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Cacheable(value = "gold", key = "'summary'")
    public Map<String, Object> summary() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT metric_key, metric_value, metric_text FROM gold_market_summary");
        return rows.stream().collect(java.util.stream.Collectors.toMap(
                r -> (String) r.get("metric_key"),
                r -> Map.of("value", r.get("metric_value"),
                            "text", String.valueOf(r.get("metric_text")))));
    }

    @Cacheable(value = "gold", key = "'areas-' + #level")
    public List<Map<String, Object>> areaPrices(String level) {
        return jdbc.queryForList(
                "SELECT level, area_code, area_name, parent_code, n_listings, "
                + "median_price_m2, mean_price_m2, p25_price_m2, p75_price_m2, "
                + "median_area, median_price FROM gold_area_price "
                + "WHERE level = ? ORDER BY median_price_m2 DESC NULLS LAST", level);
    }

    @Cacheable(value = "gold", key = "'clusters'")
    public List<Map<String, Object>> clusters() {
        return jdbc.queryForList(
                "SELECT area_code, area_name, province_name, cluster_id, cluster_label, "
                + "median_price_m2, mean_area, pct_red_book, pct_mat_tien, pca_x, pca_y "
                + "FROM gold_area_cluster ORDER BY cluster_id, median_price_m2 DESC");
    }

    @Cacheable(value = "gold", key = "'anomalies-' + #limit")
    public List<Map<String, Object>> anomalies(int limit) {
        return jdbc.queryForList(
                "SELECT listing_id, district, price, price_per_m2, predicted_price_m2, "
                + "residual_ratio, anomaly_score, reason FROM gold_anomaly "
                + "ORDER BY anomaly_score DESC LIMIT ?", limit);
    }

    @Cacheable(value = "gold", key = "'topdistricts-' + #limit")
    public List<Map<String, Object>> topDistricts(int limit) {
        return jdbc.queryForList(
                "SELECT area_code, area_name, n_listings, median_price_m2 "
                + "FROM gold_area_price WHERE level = 'district' "
                + "ORDER BY median_price_m2 DESC LIMIT ?", limit);
    }

    @Cacheable(value = "gold", key = "'forecast-' + #areaCode")
    public Map<String, Object> forecast(String areaCode) {
        String code = areaCode;
        if (code == null) {
            List<Map<String, Object>> top = jdbc.queryForList(
                    "SELECT area_code FROM gold_price_history GROUP BY 1 "
                    + "ORDER BY COUNT(*) DESC LIMIT 1");
            if (top.isEmpty()) {
                return Map.of("history", List.of(), "forecast", List.of(),
                        "metrics", List.of(),
                        "note", "Chưa có chuỗi thời gian — chạy scripts/build_price_history.py");
            }
            code = (String) top.get(0).get("area_code");
        }
        return Map.of(
                "areaCode", code,
                "history", jdbc.queryForList(
                        "SELECT ds, price_m2, source FROM gold_price_history "
                        + "WHERE area_code = ? ORDER BY ds", code),
                "forecast", jdbc.queryForList(
                        "SELECT model_name, ds, yhat, yhat_lower, yhat_upper "
                        + "FROM gold_price_forecast WHERE area_code = ? "
                        + "ORDER BY model_name, ds", code),
                "metrics", jdbc.queryForList(
                        "SELECT model_name, horizon_months, mape, rmse, mae, n_folds "
                        + "FROM gold_forecast_metrics WHERE area_code = ? ORDER BY mape", code));
    }

    /** Chất lượng ETL — bằng chứng định lượng đưa vào Chương 3 báo cáo. */
    public List<Map<String, Object>> dataQuality() {
        return jdbc.queryForList(
                "SELECT run_at, stage, rows_in, rows_out, rows_dropped, "
                + "province_match_rate, district_match_rate, ward_match_rate, notes "
                + "FROM etl_data_quality ORDER BY run_id DESC LIMIT 10");
    }
}
