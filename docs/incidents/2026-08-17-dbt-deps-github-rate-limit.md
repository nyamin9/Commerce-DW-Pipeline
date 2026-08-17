# dbt deps 가 GitHub 429 로 실패하고 기존 패키지까지 지웠다

> 2026-08-17  ·  [← 장애 기록 목록](README.md)

`dbt_deps` 태스크가 재시도 3회를 모두 소진하고 실패했다.
그리고 **실패하면서 이미 설치돼 있던 dbt_utils 까지 사라졌다.**

| | |
|---|---|
| 대상 | `thelook_dw_daily.dbt_deps` |
| 원인 분류 | 외부 의존 (네트워크·레이트리밋) |
| 영향 | dbt 를 쓰는 태스크 전부. 매크로를 못 찾아 컴파일 자체가 안 된다 |

## 1. 현상

```
15:05:01  Running with dbt=1.8.0
15:05:01  Installing dbt-labs/dbt_utils
15:05:43  Encountered an error:
External connection exception occurred: not a gzip file
...
gzip.BadGzipFile: Not a gzipped file (b'42')
```

재시도할 때는 메시지가 달랐다.

```
External connection exception occurred:
HTTPSConnectionPool(host='codeload.github.com', port=443): Read timed out. (read timeout=10.0)
```

- 태스크는 `exit code 2` 로 끝난다
- **`dbt/dbt_packages/` 가 비어 있다.** 실패 전에는 dbt_utils 가 정상 설치돼 있었다

## 2. 원인

`dbt deps` 는 hub 를 거쳐 `codeload.github.com` 에서 타르볼을 받는다.
그 요청이 **HTTP 429 (Too Many Requests)** 로 막혔다.

```
$ curl -L https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/1.4.1
429: Too Many Requests
For more on scraping GitHub and how it may affect your rights, ...
```

**`b'42'` 의 정체가 이것이다.** dbt 는 받은 바이트를 gzip 으로 열려 하는데,
내용이 `429: Too Many Requests` 라 첫 두 바이트가 `4`, `2` 였다.
"not a gzip file" 은 정확한 진단이 아니라 **에러 페이지를 타르볼로 착각한** 결과다.

### 왜 429 가 걸렸나

DAG 이 `dbt deps` 를 **매 run 마다** 부른다. 여기에 재시도 2회가 곱해지고,
장애 대응으로 run 을 여러 번 돌리면서 짧은 시간에 요청이 쌓였다.

### 왜 있던 패키지까지 사라졌나

`dbt deps` 는 **먼저 `dbt_packages/` 를 비우고 받는다.** 받기에 실패하면
정리만 되고 설치는 안 된 상태로 끝난다. 그래서 "재시도하면 되겠지" 가 아니라
**실패한 순간 프로젝트 전체가 못 도는 상태**가 된다.

- `git clone` 은 막히지 않았다. codeload 타르볼 엔드포인트만 레이트리밋 대상이다

## 3. 조치사항

### 3-1. 즉시 복구

레이트리밋에 걸리지 않는 git 경로로 받아 넣는다.

```bash
git clone --depth 1 --branch 1.4.1 https://github.com/dbt-labs/dbt-utils.git /tmp/dbt-utils
rsync -a --exclude '.git' /tmp/dbt-utils/ dbt/dbt_packages/dbt_utils/
cd dbt && dbt parse      # 매크로가 잡히는지 확인
```

- 버전은 `package-lock.yml` 의 값을 쓴다. 이 프로젝트는 `1.4.1` 이다

### 3-2. 재발 방지 — 매 run 마다 받지 않는다

[`airflow/dags/thelook_dw_daily.py`](../../airflow/dags/thelook_dw_daily.py) 의 `dbt_deps` 를
**없을 때만 받도록** 바꿨다.

```bash
cd $DBT_PROJECT_DIR && \
if [ -z "${THELOOK_DBT_DEPS_ALWAYS:-}" ] && [ -f dbt_packages/dbt_utils/dbt_project.yml ]; then
    echo "dbt_packages 가 이미 있다 — deps 를 건너뛴다"
else
    dbt deps
fi
```

- 패키지 버전은 `packages.yml` · `package-lock.yml` 로 고정돼 있어 자주 바뀌지 않는다
- 갱신이 필요하면 `THELOOK_DBT_DEPS_ALWAYS=1` 로 강제한다

### 3-3. 배운 것

- **에러 메시지가 가리키는 곳과 원인이 다를 수 있다.** "not a gzip file" 은 압축 문제처럼
  읽히지만 실제로는 HTTP 에러 본문이었다. 받은 바이트를 직접 찍어 보고서야 알았다
  - → 다음 프로젝트: 외부에서 받은 것을 파싱하다 실패하면 **받은 내용을 먼저 눈으로 볼 것**

- **매 실행마다 외부를 때리는 단계는 임계 경로에서 빼는 것이 맞다.**
  버전이 고정된 의존성을 배치마다 다시 받을 이유가 없었다. 얻는 것 없이
  실패 지점만 하나 늘리고 있었다
  - → 다음 프로젝트: "이 단계가 매번 필요한가"를 물을 것. 멱등한 것과 매번 해야 하는 것은 다르다

- **실패가 원상복구로 끝나지 않는 명령이 있다.** `dbt deps` 는 지우고 받는다.
  중간에 실패하면 시작 전보다 나쁜 상태가 된다
  - → 다음 프로젝트: 재시도를 걸기 전에 **"실패하면 이전 상태로 남는가"** 를 확인할 것.
    아니라면 재시도는 안전망이 아니다

## 관련 문서

- **[airflow/README.md](../../airflow/README.md)** — DAG 구조와 태스크 정의
- **[dbt/README.md](../../dbt/README.md)** — 패키지 관리

---

[← 장애 기록 목록](README.md)
