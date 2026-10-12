# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Bùi Đăng Khoa |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602617 |
| GitHub | dangkhoabui15092004-byte |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-02134 |

## Kết quả cuối

- `dataset_version` của bạn: `f126e2dfece7`
- Số khung hình train / val / test của bạn: `Train: 687 | Val: 158 | Test: 174 (Tổng: 1019 khung hình)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ rệt giữa telemetry nguồn A và nguồn B tại tầng Bronze gồm:
1. Tên cột: Nguồn A sử dụng tên cột chuẩn tiếng Anh (`ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`), trong khi nguồn B dùng tên viết tắt và khác biệt (`timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`).
2. Đơn vị đo lường: Tốc độ ở nguồn A là m/s, còn nguồn B đo bằng km/h; độ mở bàn đạp ở nguồn A có miền giá trị [0..1], còn nguồn B tính theo phần trăm [0..100].
3. Định dạng thời gian: Nguồn A lưu dưới dạng chuỗi ISO 8601 UTC timestamp, trong khi nguồn B sử dụng Epoch timestamp theo mili-giây (Unix millisecond).
4. Độ lệch đồng hồ: Nguồn B bị lệch đồng hồ phần cứng 1500 ms (`telemetry_clock_offset_ms = 1500`) so với video timeline, trong khi nguồn A không bị lệch.

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu tất cả cột ở dạng chữ (String) vì Bronze đóng vai trò là tầng lưu trữ nguyên bản (Raw Ingestion / Append-only) bất biến. Việc này giúp xử lý linh hoạt hiện tượng Schema Drift (cấu trúc nguồn dữ liệu thay đổi hoặc khác biệt giữa các nhà cung cấp) mà không gây vỡ bảng. Đồng thời, giữ kiểu String ngăn chặn nguy cơ mất mát dữ liệu (data loss) do parse error hoặc tự động biến đổi thành NULL âm thầm khi gặp dữ liệu dị thường; toàn bộ logic làm sạch và ép kiểu được dời sang tầng Silver theo đúng chuẩn ELT.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn về một schema thống nhất thông qua các dbt staging models (`stg_telemetry_a` và `stg_telemetry_b`):
- Chuẩn hoá cột và đơn vị: Thống nhất các tên cột chung (`ts`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `acc_x`, `acc_y`, `gas`, `brake`, `steer`). Nguồn B được quy đổi tốc độ `spd_kmh / 3.6` sang `speed_mps`, và chia bàn đạp cho 100 (`gas`, `brake`).
- Xử lý thời gian và độ lệch đồng hồ: Đọc `telemetry_clock_offset_ms` từ `bronze.video_files`, áp dụng công thức `to_timestamp((timestamp_ms - offset_ms) / 1000.0)` cho nguồn B để trừ đi 1500 ms lệch. Bằng chứng kiểm tra tự động xác nhận mốc thời gian bản ghi đầu tiên của B là `2025-10-08 14:30:00.009000+00:00`, khớp chính xác trong ngưỡng 0.5s với thời điểm bắt đầu của video B (`2025-10-08 14:30:00`), kiểm tra `drive B clock offset removed` đạt PASS.

Bảng cách ly dữ liệu lỗi (Quarantine table) thực tế:
| Drive | Lý do cách ly (`reject_reason`) | Số dòng |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng cộng mỗi drive có 53 dòng lỗi được cách ly vào `silver_telemetry_rejects`. Các dòng lỗi được đưa vào bảng cách ly thay vì bị xoá âm thầm nhằm đảm bảo tính toàn vẹn và khả năng kiểm toán (auditing) của hệ thống dữ liệu: đội ngũ kỹ sư có thể theo dõi tỷ lệ cảm biến hỏng theo thời gian, phát hiện lỗi phần cứng trên từng xe, và có thể tái xử lý (backfill) sau này khi hoàn thiện thuật toán nội suy hoặc cập nhật logic làm sạch.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` được đặt severity là `warn` vì việc mất tín hiệu cảm biến trong thời gian ngắn (dropout > 1000ms do mất GPS dưới hầm hoặc nghẽn bus CAN) là hiện tượng vật lý thường gặp trong môi trường vận hành thực tế của xe tự lái. Dữ liệu trước và sau khoảng gián đoạn vẫn hoàn toàn có giá trị sử dụng, do đó cảnh báo để giám sát mà không được làm dừng pipeline. Ngược lại, test `in_range` (tốc độ [0..70 m/s] tương đương 0..252 km/h) bắt buộc là `error` vì tốc độ xe vượt 252 km/h hoặc âm là dữ liệu phi thực tế, hư hỏng nghiêm trọng hoặc cảm biến lỗi nặng; nếu cho qua sẽ làm sai lệch mô hình lái xe tự động, gây nguy hiểm nên bắt buộc dừng pipeline tại cổng kiểm soát.

Kết quả thí nghiệm `break-gate` (`break_gate.json`): Khi nới lỏng luật lọc `max_speed_mps` từ 70 lên 1000 m/s, cổng chất lượng dbt test đã phát hiện ngay:
- Test `in_range_silver_telemetry_speed_mps__70__0` bị FAIL, bắt được chính xác **6 dòng** tốc độ bất thường (3 dòng ở drive A và 3 dòng ở drive B).
- Tóm tắt dbt test: `PASS=5 WARN=1 ERROR=1 SKIP=0`, pipeline bị chặn đứng hoàn toàn không cho dữ liệu xấu đi tiếp. Khi khôi phục luật gốc, kết quả trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.
Điều này chứng minh vai trò thiết yếu của "cổng chất lượng" (Quality Gate) như một bộ chốt chặn tự động ngăn chặn dữ liệu rác lan truyền sang các tầng hạ nguồn ("Garbage In, No Garbage Out"), bảo vệ độ tin cậy của kho dữ liệu.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào kết quả thí nghiệm `idempotency.json`:
- Số dòng trước khi chạy lại: 1246 dòng, dấu vân tay (fingerprint): `6c7480dfde7e`.
- Sau 2 lần chạy lại partition `trip-00003`: Vẫn giữ nguyên đúng **1246 dòng**, dấu vân tay `6c7480dfde7e` -> Hoàn toàn **KHÔNG ĐỔI**.
- Khi ghi theo kiểu `append` thông thường: Sau 2 lần chạy, số dòng của trip đó tăng từ 60 lên **120 dòng** (bị nhân đôi 2x).

Ghi Delta `overwrite` theo partition (predicate `trip_id = '...'`) an toàn hơn phương pháp "xoá rồi ghi lại" nhờ cơ chế transaction log (ACID) của Delta Lake:
1. Tính nguyên tử (Atomicity): Dữ liệu mới được ghi vào các file Parquet mới ngầm bên dưới, và chỉ khi hoàn tất toàn bộ thì transaction mới commit cập nhật metadata. Bảng không bao giờ bị rơi vào trạng thái rỗng hay dở dang.
2. An toàn khi có sự cố giữa chừng: Nếu tiến trình gặp sự cố (crash node, mất điện, hết bộ nhớ) trong quá trình ghi đè, transaction bị hủy bỏ và con trỏ metadata vẫn trỏ tới phiên bản cũ hợp lệ. Trong khi đó, nếu dùng phương pháp thủ công "xoá trước rồi mới ghi", một lỗi xảy ra ở giữa sẽ khiến dữ liệu cũ đã bị xoá mất vĩnh viễn còn dữ liệu mới thì chưa kịp ghi xong, gây thảm hoạ mất dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình **đen** (`is_black`, mean luma < 12) là lỗi thiết bị/camera (ống kính bị che, mất kết nối cảm biến quang), hoàn toàn không chứa thông tin thị giác nên pipeline loại bỏ triệt để. Ngược lại, khung hình **tối** (`is_dark`, 12 <= luma < 30) phản ánh điều kiện ánh sáng môi trường thực tế (xe di chuyển trong hầm tối, dưới bóng cây rậm, hoặc trời tối); những khung hình này chứa thông tin bối cảnh quý giá để mô hình học cách lái xe trong điều kiện thiếu sáng. Pipeline gắn cờ `is_dark` để phân loại nhưng **giữ lại toàn bộ** để huấn luyện. Tôi hoàn toàn đồng ý với thiết kế này vì việc huấn luyện xe tự lái rất cần dữ liệu đa dạng trong điều kiện khắc nghiệt.

Bảng thống kê số khung trùng theo ngưỡng dHash từ `threshold.json`:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn **ngưỡng 2** (như pipeline đang áp dụng). Ngưỡng 0 bit quá khắt khe, chỉ bắt được khung giống hệt nhau từng pixel và bỏ qua các khung xe đứng yên nhưng có nhiễu cảm biến nhẹ. Ngưỡng 6 bit lại quá lỏng lẻo, loại bỏ tới 363 khung ở A và 244 khung ở B, dễ xoá nhầm các khung hình có chuyển động thật của xe. Ngưỡng 2 bit (Hamming distance <= 2) là điểm cân bằng tối ưu giữa việc lọc khung đứng hình và giữ lại dữ liệu biến đổi thật. Ngưỡng này phụ thuộc chặt chẽ vào tốc độ xe: khi xe chạy chậm hoặc dừng chờ (như Drive A), cảnh vật kề nhau thay đổi rất ít nên tỷ lệ khung trùng tăng cao; khi xe chạy nhanh trên cao tốc (Drive B), cảnh vật biến đổi liên tục giữa các khung 2 fps nên số khung trùng giảm rõ rệt.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào file bằng chứng `leakage.json`:
- Khi chia **theo khung hình** (ngẫu nhiên 70/15/15): Có tới **26/26 trip** bị chia cắt (straddling), và cả **26/26 trip** đều có khung hình xuất hiện đồng thời ở cả tập Train lẫn tập Test.
- Khi chia **theo trip** (cách của pipeline Gold): **0 trip** bị chia cắt (`per_trip_split_trips_straddling = 0`), mỗi trip nằm trọn vẹn trong duy nhất một split.

Việc chia theo khung hình gây ra hiện tượng rò rỉ dữ liệu (Data Leakage) nghiêm trọng và làm điểm kiểm thử đẹp một cách giả tạo vì: Các khung hình kề nhau trong video được trích xuất ở tần số 2 fps (cách nhau chỉ 0.5 giây), do đó quang cảnh, góc lái, điều kiện thời tiết và mặt đường giữa 2 khung kề nhau gần như giống hệt nhau. Nếu một khung rơi vào tập Train và khung cách đó 0.5s rơi vào tập Test, mô hình chỉ cần "học vẹt" (memorization) hình ảnh gần giống đã thấy trong lúc huấn luyện để đoán nhãn trong tập kiểm thử. Điểm số độ chính xác (accuracy/loss) trên tập Test sẽ cao ngất ngưởng, nhưng khi mang mô hình ra đường thực tế gặp cung đường/trip hoàn toàn mới thì mô hình sẽ phán đoán sai lầm hoàn toàn.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là chuỗi **băm nội dung** (SHA-256 từ danh sách chuỗi `frame_id|split`) nhằm đảm bảo tính bất biến (immutability) và khả năng tái lập (reproducibility) chuẩn MLOps: Nếu dữ liệu đầu vào và các luật chia tách không thay đổi, hash nội dung sẽ luôn cho ra cùng một chuỗi định danh duy nhất (12 ký tự), không phụ thuộc vào thời điểm chạy, máy tính thực thi hay số lần chạy lại. Nếu dùng ngày giờ hoặc số đếm tăng dần, mỗi lần chạy lại pipeline sẽ sinh ra một phiên bản mới dù nội dung không đổi, gây nhiễu loạn việc truy vết mô hình AI.

Hai lần chạy liên tiếp với cùng cấu hình cho ra cùng một chuỗi `dataset_version` giống hệt nhau (trong bài của tôi là `f126e2dfece7`). Chuỗi `dataset_version` của tôi khác bạn bè dù dùng chung bộ video/telemetry gốc là do pipeline đưa mã số sinh viên `LAB_STUDENT_ID` (`2A202602617`) vào hàm băm để tạo "muối" khi sắp xếp thứ tự trip (`order by md5(trip_id || LAB_STUDENT_ID)`). Vì mỗi sinh viên có MSSV riêng biệt nên thứ tự phân bổ trip vào các tập train/val/test khác nhau, số khung hình ở mỗi tập khác nhau, dẫn tới chuỗi băm nội dung cuối cùng hoàn toàn độc nhất.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Từ phần `lineage` trong `manifest.json`, dataset được tạo từ:
- Hai video gốc với mã băm sha256 xác thực:
  + `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  + `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Phiên bản cụ thể của các bảng Delta upstream:
  + `bronze.raw_telemetry_a`: version 0
  + `bronze.raw_telemetry_b`: version 0
  + `bronze.trips_raw`: version 0
  + `bronze.video_files`: version 0
  + `silver.frames`: version 0
  + `silver.telemetry`: version 0
  + `silver.frames_flagged`: version 0
  + `silver.frame_telemetry`: version 0

Nếu 3 tháng sau cần tái tạo lại đúng 100% dataset này, những thông tin cốt lõi giúp tái tạo là:
1. Mã sha256 của 2 file video gốc và các file CSV thô để kiểm chứng dữ liệu đầu vào không bị sửa đổi.
2. Số phiên bản (commit version) của từng bảng Delta Lake kết hợp tính năng Time Travel (`VERSION AS OF 0`) để truy xuất đúng dữ liệu tại thời điểm xuất dataset.
3. Mã sinh viên `LAB_STUDENT_ID` (`2A202602617`) và cấu hình tỷ lệ split (70/15/15) cùng các ngưỡng lọc chất lượng (`rules`) ghi nhận trong manifest.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi thiết lập mô phỏng lỗi với `LAB_FAIL_ONCE=trip-02134`:
- Trên giao diện Airflow: Đúng **1 task** trích xuất con trong nhóm `silver_frames` (tương ứng với trip-02134) bị chuyển sang màu **đỏ** (`failed`) với thông báo lỗi `RuntimeError: simulated crash while extracting trip-02134; run it again`.
- Toàn bộ **7 task** hạ nguồn phụ thuộc (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) đều chuyển sang trạng thái màu cam **`upstream_failed`**.
- Thao tác phục hồi: Tôi chỉ cần nhấn **Clear** đúng duy nhất task bị đỏ đó. Airflow tự động chạy lại partition bị lỗi mà không cần chạy lại các trip khác.
- Dữ liệu hoàn toàn **không bị nhân đôi** nhờ cơ chế Delta Lake `overwrite` theo partition predicate `trip_id = 'trip-02134'`.
- `dataset_version` sau khi DAG xanh hoàn toàn vẫn giữ nguyên là `f126e2dfece7`.
- Bằng chứng: Ảnh chụp `airflow_crash_grid.png` thể hiện rõ cột chạy đầu tiên bị đỏ ở `silver_frames` và cột chạy sau chuyển xanh lá cây hoàn toàn; kiểm tra tự động sau đó đạt 27/27 checks passed.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi quy mô tăng lên 500 xe/ngày (khoảng 5.000 giờ video và hàng trăm triệu bản ghi telemetry mỗi ngày), ba thành phần sẽ bị nghẽn và vỡ đầu tiên là:
1. **Xử lý giải mã video tập trung (FFmpeg trên Airflow LocalExecutor)**: Một node đơn lẻ không thể giải mã hàng ngàn giờ video. Cần thay bằng cụm xử lý phân tán sử dụng Apache Spark (hoặc Ray) kết hợp phần cứng tăng tốc GPU/NVDEC (Hardware-accelerated video decoding), phân bổ tác vụ qua Kubernetes Executor (slide 23, 24).
2. **Engine phân tích DuckDB single-node**: DuckDB xử lý in-process trên file cục bộ (`lakehouse.duckdb`), sẽ bị tràn RAM và nghẽn disk I/O khi dữ liệu lên tới hàng terabyte. Cần thay bằng Distributed Query Engine như Apache Spark, Trino hoặc Databricks Photon chạy trực tiếp trên kho dữ liệu mở Delta Lake / Iceberg (slide 12, 22).
3. **Serving Warehouse PostgreSQL**: PostgreSQL truyền thống không thiết kế cho khối lượng ingestion hàng tỷ dòng time-series và phân tích OLAP quy mô lớn. Cần thay thế bằng Cloud Data Warehouse hiện đại như Snowflake, Google BigQuery, ClickHouse hoặc truy vấn trực tiếp trên Delta Lakehouse (slide 16).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select 
    t.trip_id,
    t.drive_id,
    t.frames_kept,
    round(100.0 * (f.dropped_black + f.dropped_blurry + f.dropped_near_dup + f.dropped_out_of_sync) / nullif(t.frames_kept + f.dropped_black + f.dropped_blurry + f.dropped_near_dup + f.dropped_out_of_sync, 0), 2) as drop_rate_pct,
    t.lighting,
    t.conditions
from marts.mart_trip_summary t
join marts.mart_frame_funnel f on t.drive_id = f.drive_id
order by drop_rate_pct desc
limit 10;
```
*Điều câu truy vấn cho biết*: Câu truy vấn phân tích tỷ lệ phần trăm khung hình bị loại bỏ (`drop_rate_pct`) trên từng trip cùng điều kiện ánh sáng và thời tiết tương ứng. Kết quả giúp kỹ sư vận hành nhận diện chính xác các chuyến xe hoặc đoạn đường có chất lượng camera kém nhất (ví dụ: camera bị che hoặc xe dừng đỗ quá lâu), từ đó có kế hoạch bảo trì cảm biến kịp thời cho từng xe.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Điều bất ngờ nhất: Cơ chế chia split theo trip giải quyết triệt để vấn đề rò rỉ dữ liệu (data leakage); nếu chia theo frame thì 100% trip bị rò rỉ và mô hình sẽ ghi nhớ thay vì học tổng quát.
- Điều chưa rõ: Khi đồng bộ ASOF join giữa khung hình và telemetry, việc chọn bản ghi telemetry kề trước (latest at or before) có thể tạo ra độ trễ nhỏ nếu tần số telemetry thấp, liệu phương pháp nội suy tuyến tính (linear interpolation) có đem lại độ chính xác cao hơn không?
- Lỗi dữ liệu pipeline chưa bắt được: Giọt nước mưa, vết bẩn bám cục bộ trên kính chắn gió hoặc hiện tượng chói loá ánh sáng mặt trời ngược chiều (sun glare/lens flare) làm che khuất vật thể nhưng không bị cờ `is_black` hay Laplacian blur (`is_blurry`) phát hiện.
