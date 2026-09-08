package vn.uit.ie221.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

/**
 * Đọc speed view của hot path trong Redis.
 *
 * <p>KHÔNG cache ở đây, khác hẳn {@link GoldRepository}. Cả điểm của hot path
 * là độ tươi tính bằng giây; đặt cache 60 giây lên nó thì mọi công sức của
 * Spark Structured Streaming thành vô nghĩa.
 */
@Repository
public class HotPathRepository {

    private final StringRedisTemplate redis;

    public HotPathRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Các quận sôi động nhất trong cửa sổ trượt gần nhất. */
    public List<Map<String, Object>> velocity(int limit) {
        Set<ZSetOperations.TypedTuple<String>> top =
                redis.opsForZSet().reverseRangeWithScores("hot:top_districts", 0, limit - 1);
        List<Map<String, Object>> out = new ArrayList<>();
        if (top == null) {
            return out;
        }
        for (ZSetOperations.TypedTuple<String> t : top) {
            String member = t.getValue();
            if (member == null) {
                continue;
            }
            String code = member.split(java.util.regex.Pattern.quote("|"))[0];
            Map<Object, Object> h = redis.opsForHash().entries("hot:velocity:" + code);
            Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("districtCode", code);
            row.put("nListings", t.getScore() == null ? 0 : t.getScore().intValue());
            h.forEach((k, v) -> row.put(String.valueOf(k), v));
            out.add(row);
        }
        return out;
    }

    /** Cảnh báo tin lệch giá, mới nhất trước. Chuỗi JSON giữ nguyên. */
    public List<String> alerts(int limit) {
        List<String> raw = redis.opsForList().range("hot:alerts", 0, limit - 1);
        return raw == null ? List.of() : raw;
    }

    public long alertTotal() {
        String v = redis.opsForValue().get("hot:alerts:total");
        return v == null ? 0L : Long.parseLong(v);
    }

    public boolean isUp() {
        try {
            return Boolean.TRUE.equals(redis.hasKey("hot:meta"))
                    || redis.opsForZSet().size("hot:top_districts") != null;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
