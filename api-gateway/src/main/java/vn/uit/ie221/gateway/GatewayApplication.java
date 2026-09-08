package vn.uit.ie221.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Điểm vào của API Gateway.
 *
 * <p>Ba việc gateway làm mà không tầng nào khác làm được:
 * <ol>
 *   <li><b>Gộp nguồn</b> — một request của dashboard cần dữ liệu từ PostgreSQL,
 *       Redis và FastAPI cùng lúc. Gộp ở đây thì frontend chỉ gọi một lần.</li>
 *   <li><b>Cache</b> — tầng Gold chỉ đổi khi job Spark chạy, nên đọc lặp là
 *       lãng phí. Cache ở gateway phục vụ mọi client, cache ở trình duyệt thì
 *       không.</li>
 *   <li><b>Che topology</b> — frontend không cần biết Redis hay FastAPI tồn
 *       tại. Đổi hạ tầng phía sau không phải sửa frontend.</li>
 * </ol>
 */
@SpringBootApplication
@EnableCaching
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    /** Dashboard chạy ở cổng khác (5173) nên trình duyệt coi là cross-origin. */
    @Bean
    WebMvcConfigurer corsConfig() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**").allowedOriginPatterns("*")
                        .allowedMethods("GET", "POST");
            }
        };
    }
}
