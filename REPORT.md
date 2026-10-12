# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phạm Văn Hoàng Anh Tú |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602507 |
| GitHub | tubepvhat1604-bot |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-00722 |

> Ghi chú về trip: MSSV có chữ `A` nên lệnh `int(MSSV) % 26` trong GUIDE báo lỗi. Mình lấy phần số `2202602507 % 26 = 1`, ứng với `trip-00722` (đọc MSSV như số hệ 16 cũng ra cùng trip này).

## Kết quả cuối

- `dataset_version` của bạn: `c6ffad7a3236`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 747 / 161 / 114 (trip: 18 / 4 / 4; tổng 1022 khung)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt giữa telemetry A và B ở bronze (2 dòng mẫu mà `step 1` in ra):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Bàn đạp**: A là tỉ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: B lệch so với video, `bronze.video_files` ghi `telemetry_clock_offset_ms` = 1500 cho drive_b và 0 cho drive_a.

Số dòng: A 3021, B 3234. Nạp lần hai báo `already present` (nhận diện bằng sha256), `history bronze.raw_telemetry_a` chỉ có **1 commit** (`rows_added: 3021`).
Giữ mỗi nguồn một bảng và để mọi cột là chữ vì bronze phải là **bản sao trung thực** của nguồn: nếu ép kiểu ngay thì một ô hỏng (ví dụ tốc độ trống) có thể làm hỏng cả lần nạp hoặc bị đổi thành NULL mà không ai biết, và khi luật chuẩn hoá sai thì không còn dữ liệu gốc để chạy lại. Hai schema khác nhau nên gộp ở bronze sẽ phải đoán ánh xạ cột quá sớm; việc đó để cho silver, nơi có thể sửa và chạy lại từ bronze.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Trong `stg_telemetry_b.sql`, B được đưa về cùng schema với A: đổi tên cột, `spd_kmh / 3.6` để ra m/s, `throttle_pct / 100` để về 0..1, `to_timestamp((timestamp_ms - offset_ms) / 1000)` để từ epoch ms sang timestamp UTC **và trừ độ lệch đồng hồ** 1500 ms lấy từ `bronze.video_files`. Ô trống được `try_cast(nullif(...))` thành NULL để đem đi kiểm tra thay vì làm sập.

Bằng chứng: sau chuẩn hoá, `bat_dau` của drive_b là `2025-10-08 14:30:00.009`, khớp thời điểm bắt đầu video (lệch 9 ms); tốc độ tối đa hợp lý (A 71 km/h, B 100 km/h). Verify module 2 đạt 7/7.

Bảng quarantine của mình:

| drive | duplicate | speed_missing | speed_out_of_range | tổng bị cách ly | dòng sạch |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 (2968 + 53 = 3021) |
| drive_b | 20 | 30 | 3 | 53 | 3181 (3181 + 53 = 3234) |

Dòng lỗi vào `silver_telemetry_rejects` (kèm `reject_reason`, `_batch_id`, `_row_no`) thay vì bị xoá để: (1) đếm được và kiểm tra được "sạch + cách ly = nhận về", không mất dòng âm thầm; (2) truy ngược được về đúng dòng trong file nguồn để báo cho bên cung cấp; (3) nếu luật sai (ví dụ ngưỡng tốc độ quá chặt) thì có thể sửa luật và lấy lại dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống giữa hai mẫu > 1000 ms) là `warn` vì mất tín hiệu cảm biến vài giây là chuyện **có thật và chấp nhận được**: dữ liệu vẫn đúng, chỉ thưa hơn; dừng cả pipeline vì nó thì không bao giờ ra được dataset. Ở bước 2, `dbt test` cho PASS=6 **WARN=1** ERROR=0. Còn `in_range` (tốc độ 0..70 m/s) là `error` vì tốc độ vô lý là dữ liệu **sai**, nếu lọt vào silver sẽ làm sai nhãn và ghép telemetry cho khung hình, nên phải chặn.

Thí nghiệm `break-gate` (`break_gate.json`): nới luật làm sạch lên 1000 m/s thì `dbt test` ra PASS=5 WARN=1 **ERROR=1**; test bắt được là `in_range_silver_telemetry_speed_mps__70__0` với **6 dòng** (đúng 3 dòng vô lý mỗi drive đã lọt qua). Khôi phục luật thì về PASS=6 WARN=1 ERROR=0.
Điều này cho thấy cổng chất lượng là lớp bảo vệ **độc lập** với luật làm sạch: kể cả khi ai đó sửa nhầm luật ở transform, test vẫn chặn dữ liệu xấu trước khi đi tiếp sang gold (trong Airflow, `dbt_test_silver` lỗi thì các task sau không chạy).

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` (trip `trip-00003`, 60 khung):
- Trước: **1246 dòng**, dấu vân tay `7b822c530dcf`.
- Sau 2 lần chạy lại partition: **1246 dòng**, dấu vân tay `7b822c530dcf`, không đổi (`unchanged: true`).
- Cách sai (ghi `append` 2 lần cho một trip): **120 dòng** cho trip chỉ có 60 khung, tức nhân đôi.

Delta `overwrite` theo partition (`replaceWhere trip_id = ...`) là **một commit nguyên tử**: xoá phần cũ của trip và ghi phần mới nằm trong cùng một giao dịch. Nếu job chết giữa chừng thì commit không xảy ra, người đọc vẫn thấy phiên bản cũ đầy đủ. Còn "xoá rồi ghi lại" là hai thao tác riêng: chết sau khi xoá và trước khi ghi xong thì trip đó **mất dữ liệu** hoặc chỉ còn một nửa. Ngoài ra overwrite chỉ chạm đúng trip đó nên chạy lại một trip không ảnh hưởng 25 trip còn lại, và có time travel để quay lại nếu cần.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen** là khung hỏng (camera/encoder lỗi, gần như toàn pixel đen): không có thông tin, pipeline **loại** khỏi gold. **Khung tối** là cảnh thật nhưng thiếu sáng (đường hầm, bóng râm): pipeline chỉ **gắn cờ và giữ lại**. Số của mình: drive_a 6 đen, 4 tối giữ lại; drive_b 4 đen, **61 tối giữ lại**. Mình đồng ý: mô hình xe tự lái phải học cả cảnh thiếu sáng; bỏ chúng thì dataset lệch về điều kiện đẹp, và cờ vẫn cho phép lọc sau nếu cần.

`threshold.json` (số khung bị coi là trùng):

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 58 | 8 |
| 2 | 146 | 53 |
| 6 | 366 | 246 |

Mình chọn **ngưỡng 2** (như pipeline): ngưỡng 0 chỉ bắt khung giống y hệt nên bỏ sót đoạn camera đứng hình có nhiễu nhẹ; ngưỡng 6 loại tới 366/602 khung của A (61%), tức đã xoá cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ xe: xe chạy chậm (A có trip 6,5 km/h và đoạn đứng hình 6 giây) thì các khung liền nhau rất giống, số trùng tăng nhanh; xe chạy nhanh (B trung bình ~70 km/h) thì cảnh đổi nhiều nên ít trùng. Vì vậy cùng ngưỡng 2, A bị 146 còn B chỉ 53; nếu muốn chuẩn hơn có thể cho ngưỡng thấp hơn khi tốc độ thấp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: có 1022 khung đáng dùng thuộc 26 trip.
- Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** bị chia cắt, cả 26 trip đều có khung ở **cả train lẫn test**.
- Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt.

Lấy mẫu 2 khung/giây nên hai khung liền nhau gần như giống hệt (cùng đường, cùng xe phía trước, cùng ánh sáng). Nếu một khung ở train và khung bên cạnh ở test thì lúc đánh giá mô hình chỉ cần "nhớ" cảnh đã thấy, không cần tổng quát hoá. Điểm test sẽ cao giả tạo và sụp khi gặp một con đường mới. Chia theo trip đảm bảo test là những cảnh mô hình chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là băm nội dung (danh sách khung, split, quy tắc) nên **cùng nội dung thì cùng version, khác nội dung thì khác version**. Ngày giờ hay số đếm tăng dần không nói gì về nội dung: chạy lại y hệt cũng ra version mới, còn hai dataset khác nhau vẫn có thể trùng số đếm ở hai máy. Hai lần chạy liên tiếp của mình (`step 4` rồi `cli export-dataset`, và cả lần `step all --reset` cuối) đều ra **`c6ffad7a3236`**, kể cả khi lineage ghi phiên bản bảng `silver.frames` khác nhau (27 trước khi reset, 25 sau khi reset), vì nội dung khung không đổi.
MSSV được trộn vào thứ tự xếp trip (`md5(trip_id || MSSV)`), nên trip nào vào train/val/test thay đổi theo MSSV. Mình thử đặt MSSV `2A202602508` thì ra version `ec836294fb37` với train/val/test = 698/179/145, thay vì `c6ffad7a3236` với 747/161/114. Cùng dữ liệu nhưng split khác thì nội dung khác, nên version khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Phần `lineage` trong `gold/datasets/c6ffad7a3236/manifest.json` (lần chạy cuối):
```
bronze.raw_telemetry_a: 0, bronze.raw_telemetry_b: 0, bronze.trips_raw: 0, bronze.video_files: 1,
silver.frames: 25, silver.telemetry: 0, silver.frames_flagged: 0, silver.frame_telemetry: 0,
input_video_sha256: drive_a 636ba1a9…e7e9aa, drive_b bfa62703…bee9fdc
```
Nghĩa là: dataset này được tạo từ hai video có đúng sha256 trên, telemetry thô A/B và bảng trips ở phiên bản Delta 0, bảng `silver.frames` ở phiên bản 25 (26 commit, mỗi trip một commit), các bảng silver dbt ở phiên bản 0. `rules` ghi rõ luật: loại `is_black, is_blurry, is_near_dup, not sync_ok`, chia "per trip, stratified by drive, md5 order".
Ba tháng sau, để tái tạo đúng dataset này cần: sha256 của video gốc (kiểm tra đúng file), số phiên bản từng bảng Delta (time travel về đúng phiên bản đó), quy tắc lọc/chia, MSSV dùng làm salt, và `manifest_sha256` (`a8567d3b…a0338`) để so lại kết quả.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của mình là **trip-00722** (map index 1 của `silver_frames`). Sau khi đặt `LAB_FAIL_ONCE=trip-00722`, chạy `day09_reset` rồi `day09_video_pipeline`:
- **1 task chuyển đỏ**: `silver_frames` map index 1, log ghi `RuntimeError: simulated crash while extracting trip-00722; run it again` (ảnh `fail_task_log.png`).
- 25 task `silver_frames` còn lại vẫn **xanh**; **7 task** phía sau chuyển `upstream_failed` (`dbt_run_silver, dbt_test_silver, dbt_run_gold, dbt_test_gold, publish_lake, export_dataset, publish_warehouse`). Ảnh `fail_graph.png`.
- Mình chỉ clear task đỏ (kèm các task phía sau đang `upstream_failed`): task trip-00722 chạy lại ở **attempt 2** và thành công, 7 task sau chạy lại; 25 trip kia **không** chạy lại. DAG xanh, 37/37 task success (ảnh `airflow_retry_graph.png`).
- `scripts/lab.sh verify` sau đó: **27/27**, số khung silver vẫn 1246 (`frame_id unique`), gallery 1022 = gold, nên **không nhân đôi**; `dataset_version` vẫn **`c6ffad7a3236`**, không đổi.
Lý do: mỗi trip là một partition ghi bằng overwrite nên chạy lại một trip là idempotent.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Với 500 xe mỗi ngày (thay vì 2 drive ~5 phút), ba chỗ vỡ đầu tiên:
1. **Giải mã video bằng FFmpeg trong một container Airflow** (`silver_frames`, PARALLELISM=2, CPU): decode là nút cổ chai (slide 24). Thay bằng hàng đợi + cụm worker phân tán (Spark/Ray, Kubernetes executor) và decode bằng GPU (NVDEC/DALI), chỉ trích khung cần dùng.
2. **DuckDB + dbt chạy trên một máy** làm engine biến đổi: dữ liệu telemetry và bảng khung hình sẽ lên hàng tỉ dòng, vượt RAM một node. Thay bằng engine phân tán trên lakehouse (Spark/Databricks, Trino) đọc trực tiếp bảng Delta, partition theo ngày/xe (slide 22, 23).
3. **Airflow với một task cho mỗi trip và một scheduler LocalExecutor**: hàng chục nghìn trip/ngày sẽ làm scheduler và metadata DB quá tải. Chuyển sang xử lý theo lô/partition ngày, CeleryExecutor/KubernetesExecutor, hoặc ingest theo luồng sự kiện (Kafka) thay vì batch thủ công (slide 12).
Ngoài ra MinIO một node và Postgres+Metabase cho dashboard cũng cần thay bằng object storage cloud (S3) và warehouse cột (BigQuery/Snowflake).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi 1: Trip nào bị loại nhiều khung nhất?**
```sql
select trip_id, drive_id, round(100.0*(frames-frames_kept)/frames,1) as drop_pct,
       frames - frames_kept as dropped, frames, round(avg_speed_kmh::numeric,1) as avg_speed_kmh
from marts.mart_trip_summary order by drop_pct desc limit 10
```
Kết quả: đứng đầu là `trip-04622` (56,7%, 34/60 khung), `trip-03904` (50%), `trip-02086` (46,7%), `trip-08888` (40%, tốc độ chỉ 6,5 km/h), `trip-07774` (38,2%). 8/10 trip đứng đầu là của **drive_a**, khớp với funnel: drive_a có 146 khung gần trùng, 9 mờ, 6 đen (đoạn camera đứng hình và xe chạy chậm), còn drive_b nhanh hơn nên ít bị loại.

**Câu hỏi 2 (vận hành): Tỷ lệ loại khung theo dải tốc độ trung bình của trip**
```sql
select case when avg_speed_kmh < 20 then '1. <20 km/h' when avg_speed_kmh < 50 then '2. 20-50 km/h' else '3. >=50 km/h' end as speed_band,
       count(*) as trips, sum(frames) as frames, sum(frames-frames_kept) as dropped,
       round(100.0*sum(frames-frames_kept)/sum(frames),1) as drop_pct
from marts.mart_trip_summary group by 1 order by 1
```
Kết quả: < 20 km/h loại **32,4%** (73/225 khung, 5 trip); 20–50 km/h loại 17,1%; ≥ 50 km/h loại 14,0%. Trip chạy chậm mất gấp đôi khung, chủ yếu do lọc khung gần trùng. Về vận hành, nếu cần dataset cân bằng tốc độ thì nên lấy mẫu dày hơn ở đoạn chạy nhanh hoặc nới ngưỡng dHash cho đoạn chạy chậm. Cả hai câu nằm ở hàng đầu của dashboard (ảnh `metabase_dashboard.png`).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: chia theo khung hình làm **cả 26/26 trip** bị rò rỉ sang test. Mình nghĩ chỉ vài trip thôi, nhưng chia ngẫu nhiên theo khung thì gần như chắc chắn trip nào cũng bị cắt. Cũng bất ngờ là `dataset_version` giữ nguyên dù phiên bản bảng `silver.frames` trong lineage khác nhau.
- **Chưa rõ**: vì sao kiểm tra "frozen camera" chỉ bắt 11/12 khung đứng hình ở drive_a (vẫn PASS); có thể khung đầu tiên của đoạn đứng hình không có khung trước giống nó.
- **Lỗi pipeline chưa bắt**: telemetry chỉ kiểm tra tốc độ trống/vô lý, chưa kiểm tra **tốc độ nhảy bất thường** giữa hai mẫu liền nhau (ví dụ 10 → 60 m/s trong 0,1 s) hay GPS nhảy xa; mấy giá trị đó vẫn nằm trong 0..70 nên lọt qua `in_range`. Ngoài ra MSSV có chữ (`2A…`) làm lệnh tính trip ở GUIDE (`int(MSSV) % 26`) bị lỗi; mình phải lấy phần số.
