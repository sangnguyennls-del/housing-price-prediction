package vn.uit.ie221.gateway;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Toàn bộ mặt API mà dashboard nhìn thấy. */
@RestController
@RequestMapping("/api")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final GoldRepository gold;
    private final HotPathRepository hot;
    private final MlApiClient ml;

    public DashboardController(GoldRepository gold, HotPathRepository hot, MlApiClient ml) {
        this.gold = gold;
        this.hot = hot;
        this.ml = ml;
    }

    /**
     * Một request cho toàn bộ màn hình chính.
     *
     * <p>Đây là lý do tồn tại của gateway, gói gọn trong một endpoint: dữ liệu
     * đến từ PostgreSQL (KPI, top quận, phân cụm) VÀ Redis (hot path) — hai kho
     * hoàn toàn khác nhau, hợp nhất tại đây thay vì bắt trình duyệt tự ghép.
     *
     * <p>Hot path hỏng KHÔNG được làm hỏng cả màn hình: phần đó được bọc riêng
     * và suy biến thành danh sách rỗng kèm cờ báo, còn phần Gold vẫn hiện đủ.
     */
    @GetMapping("/dashboard/overview")
    public Map<String, Object> overview() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("summary", gold.summary());
        out.put("topDistricts", gold.topDistricts(10));
        out.put("clusters", gold.clusters());

        Map<String, Object> hotBlock = new LinkedHashMap<>();
        try {
            hotBlock.put("velocity", hot.velocity(10));
            hotBlock.put("alertTotal", hot.alertTotal());
            hotBlock.put("available", true);
        } catch (RuntimeException e) {
            log.warn("Hot path không đọc được: {}", e.toString());
            hotBlock.put("velocity", List.of());
            hotBlock.put("alertTotal", 0);
            hotBlock.put("available", false);
            hotBlock.put("reason", "Redis chưa sẵn sàng hoặc streaming chưa chạy");
        }
        out.put("hot", hotBlock);
        return out;
    }

    @GetMapping("/areas/price")
    public List<Map<String, Object>> areaPrices(
            @RequestParam(defaultValue = "district") String level) {
        if (!List.of("province", "district", "ward").contains(level)) {
            throw new IllegalArgumentException("level phải là province|district|ward");
        }
        return gold.areaPrices(level);
    }

    @GetMapping("/clusters")
    public List<Map<String, Object>> clusters() {
        return gold.clusters();
    }

    @GetMapping("/anomalies")
    public List<Map<String, Object>> anomalies(
            @RequestParam(defaultValue = "50") int limit) {
        return gold.anomalies(Math.min(Math.max(limit, 1), 500));
    }

    @GetMapping("/forecast")
    public Map<String, Object> forecast(@RequestParam(required = false) String areaCode) {
        return gold.forecast(areaCode);
    }

    @GetMapping("/quality")
    public List<Map<String, Object>> quality() {
        return gold.dataQuality();
    }

    @GetMapping("/hot/velocity")
    public List<Map<String, Object>> velocity(@RequestParam(defaultValue = "20") int limit) {
        return hot.velocity(Math.min(Math.max(limit, 1), 100));
    }

    @GetMapping("/hot/alerts")
    public Map<String, Object> alerts(@RequestParam(defaultValue = "50") int limit) {
        return Map.of("total", hot.alertTotal(),
                      "alerts", hot.alerts(Math.min(Math.max(limit, 1), 200)));
    }

    /** Chuyển tiếp sang tầng suy luận. Gateway không tự tính giá. */
    @PostMapping("/predict")
    public ResponseEntity<Map<String, Object>> predict(@RequestBody Map<String, Object> body) {
        return proxy(() -> ml.predict(body));
    }

    @PostMapping("/anomaly")
    public ResponseEntity<Map<String, Object>> anomaly(@RequestBody Map<String, Object> body) {
        return proxy(() -> ml.anomaly(body));
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            out.put("postgres", !gold.summary().isEmpty() ? "ok" : "empty");
        } catch (RuntimeException e) {
            out.put("postgres", e.getClass().getSimpleName());
        }
        out.put("redis", hot.isUp() ? "ok" : "empty");
        out.put("mlapi", ml.health());
        return out;
    }

    /**
     * Lỗi của tầng suy luận trả về nguyên trạng thái, không nuốt thành 500.
     * FastAPI trả 503 kèm "chưa nạp được mô hình" — thông tin đó phải tới được
     * người dùng, nếu không thì mọi sự cố đều trông giống nhau.
     */
    private ResponseEntity<Map<String, Object>> proxy(java.util.function.Supplier<Map<String, Object>> call) {
        try {
            return ResponseEntity.ok(call.get());
        } catch (org.springframework.web.client.RestClientResponseException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .body(Map.of("error", "tầng suy luận từ chối",
                                 "detail", e.getResponseBodyAsString()));
        } catch (RuntimeException e) {
            log.error("Không gọi được api-ml", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "không gọi được tầng suy luận",
                                 "detail", e.getClass().getSimpleName()));
        }
    }
}
