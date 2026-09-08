package vn.uit.ie221.gateway;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Client gọi tầng suy luận FastAPI.
 *
 * <p>Gateway là nơi DUY NHẤT biết địa chỉ của api-ml. Nhờ vậy đổi cổng, tách
 * thành nhiều bản sao, hay thay bằng dịch vụ khác đều không đụng tới frontend.
 *
 * <p>Có timeout tường minh: suy luận là gọi mạng ra ngoài tiến trình, mà một
 * lời gọi treo không giới hạn sẽ giữ chặt luồng của Tomcat và kéo sập cả
 * gateway — kể cả những endpoint chỉ đọc PostgreSQL.
 */
@Component
public class MlApiClient {

    private static final Logger log = LoggerFactory.getLogger(MlApiClient.class);

    private final RestClient client;

    public MlApiClient(@Value("${app.mlapi.baseUrl}") String baseUrl,
                       @Value("${app.mlapi.timeoutMs}") int timeoutMs) {
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(Duration.ofMillis(timeoutMs));
        f.setReadTimeout(Duration.ofMillis(timeoutMs));
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(f).build();
        log.info("Tầng suy luận: {} (timeout {} ms)", baseUrl, timeoutMs);
    }

    public Map<String, Object> predict(Map<String, Object> body) {
        return post("/predict", body);
    }

    public Map<String, Object> anomaly(Map<String, Object> body) {
        return post("/anomaly", body);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> post(String path, Map<String, Object> body) {
        return client.post().uri(path).contentType(MediaType.APPLICATION_JSON)
                .body(body).retrieve().body(Map.class);
    }

    /** Trạng thái tầng suy luận; không ném ngoại lệ để /health luôn trả lời được. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> health() {
        try {
            return client.get().uri("/health").retrieve().body(Map.class);
        } catch (RuntimeException e) {
            return Map.of("status", "down", "error", e.getClass().getSimpleName());
        }
    }
}
