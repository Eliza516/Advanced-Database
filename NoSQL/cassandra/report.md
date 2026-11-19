# Báo cáo Lab: Column-Family Database (Cassandra) — SensorRing

## 1. Token của `hn-01` giống nhau trên mọi máy, nhưng IP node sở hữu lại khác?
- Cassandra sử dụng hàm băm **Murmur3Partitioner** cố định, nên cùng một partition key `"hn-01"` luôn cho ra cùng một giá trị token 64-bit (`-6927399588689370405`) trên mọi máy.
- Ngược lại, **IP của các container** do Docker cấp phát động trong mạng bridge nội bộ, và **dải token (vnodes)** của mỗi node được sinh ngẫu nhiên khi node gia nhập ring lần đầu. Do đó, node sở hữu token kế tiếp theo chiều kim đồng hồ trên máy bạn sẽ có IP và chủ sở hữu khác với ví dụ trong tài liệu.

---

## 2. Thêm node `cass4` vào cluster với keyspace `ring1`: chuyện gì xảy ra với các partition hiện có? Toàn bộ dữ liệu có phải di chuyển không?
- Khi thêm `cass4`, node mới nhận một tập các vnode (mặc định 16 token) rải đều trên token ring, tức là nó chia lại một phần khoảng token từ các node hiện có (`cass1`, `cass2`, `cass3`).
- **Không phải toàn bộ dữ liệu đều di chuyển.** Nhờ **consistent hashing**, chỉ những partition có token rơi vào các khoảng được gán mới cho `cass4` (khoảng ~1/4 dữ liệu với cluster 4 node) mới được stream sang `cass4`; các partition còn lại vẫn nằm nguyên ở node cũ.

---

## 3. Số dòng đọc trong `ALLOW FILTERING` (Step 2) và thiết kế bảng mới không cần `ALLOW FILTERING`
- Với 30 sensor × 100 dòng = **3.000 dòng**: truy vấn theo `sensor_id = 'hn-01' LIMIT 3` chỉ thực hiện **single-partition query** và đọc đúng **3 dòng**. Trong khi đó, truy vấn `WHERE temperature > 31.5 ALLOW FILTERING` phải quét tuần tự (`seq scan`) qua tất cả các token range trên cả ring, đọc toàn bộ **~3.000 dòng** chỉ để lọc ra khoảng ~120–150 dòng thỏa điều kiện (tùy dữ liệu ngẫu nhiên).
- **Thiết kế bảng trả lời *"readings above 31.5°C on a given day"* không cần `ALLOW FILTERING`:**
  Dùng `reading_date` làm **partition key**, và `temperature` làm **clustering key** đầu tiên (sắp xếp giảm dần để lấy nhanh các giá trị > 31.5°C bằng range scan trong partition):

```sql
CREATE TABLE iot.high_temp_by_day (
    reading_date date,
    temperature  double,
    ts           timestamp,
    sensor_id    text,
    humidity     double,
    PRIMARY KEY ((reading_date), temperature, ts, sensor_id)
) WITH CLUSTERING ORDER BY (temperature DESC, ts DESC, sensor_id ASC);

-- Truy vấn không cần ALLOW FILTERING:
SELECT sensor_id, ts, temperature, humidity
FROM iot.high_temp_by_day
WHERE reading_date = '2026-10-22' AND temperature > 31.5;
```

---

## 4. Bảng tổng hợp kết quả Step 3 (`RF = 3`) và bài toán `RF = 5`

| Số node sống (Live nodes) | `ONE` (cần 1) | `QUORUM` (cần 2) | `ALL` (cần 3) |
| :--- | :---: | :---: | :---: |
| **3 alive** (`cass1`, `cass2`, `cass3`) | OK | OK | OK |
| **2 alive** (dừng `cass3`) | OK | OK | `Unavailable` (cần 3, sống 2) |
| **1 alive** (dừng `cass2`, `cass3`) | OK | `Unavailable` (cần 2, sống 1) | `Unavailable` (cần 3, sống 1) |

- Với **`RF = 5`**:
  - `QUORUM = floor(5 / 2) + 1 = 3` replicas.
  - Số node lỗi tối đa chịu được mà `QUORUM` vẫn hoạt động: `5 - 3 = 2` node.

---

## 5. Step 4: Nếu cả ghi và đọc đều dùng `QUORUM`, có đọc phải dữ liệu cũ (stale data) không?
- **Không** (với điều kiện cả hai thao tác đều thành công).
- Theo nguyên lý giao nhau của quorum: `R + W > RF`.
  - Với `RF = 3`, `W = QUORUM = 2`, `R = QUORUM = 2` -> `R + W = 4 > 3`.
  - Tập hợp 2 replica được ghi thành công và tập hợp 2 replica được đọc bắt buộc phải **giao nhau ở ít nhất 1 replica** (`2 + 2 - 3 = 1`). Coordinator so sánh timestamp giữa các bản trả về và luôn chọn bản có **write timestamp mới nhất**, đồng thời kích hoạt *read repair* nếu phát hiện replica cũ.

---

## 6. Hệ thống IoT chấp nhận mất vài reading nhưng không bao giờ được ngừng nhận dữ liệu: chọn Consistency Level nào cho ghi?
- Chọn **`ONE`** (hoặc **`ANY`** nếu muốn tận dụng cả hinted handoff khi mọi replica đều down).
- **Lý do:** Theo định lý **CAP**, ưu tiên **Availability (tính sẵn sàng)** hơn Strong Consistency. Chỉ cần 1 replica (với `ONE`) hoặc 1 node bất kỳ lưu hint (với `ANY`) còn sống thì thao tác ghi vẫn thành công và độ trễ thấp, đảm bảo luồng dữ liệu từ hàng nghìn cảm biến không bị gián đoạn khi có node bảo trì hoặc gặp sự cố.
