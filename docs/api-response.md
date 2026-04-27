# API Response — Non-Admin (Viewer) Access

Daftar lengkap response semua endpoint API yang bisa di-fetch oleh user non-admin (role: `viewer`). Endpoint bertanda ❌ = admin-only (viewer akan menerima `403 insufficient permissions`).

## Akses Role `viewer`

Viewer dapat mengakses seluruh endpoint **public** (auth/health) + endpoint **GET** yang di-guard `RequireRole(RoleViewer)`. Semua endpoint **mutation** (`POST`, `DELETE`) dengan `RequireRole(RoleAdmin)` akan ditolak.

Header auth: `Authorization: Bearer <token>` atau `x-api-key: <key>`.

---

## 1) AUTH & HEALTH (public)

### `POST /api/auth/login` — `POST /api/v2/auth/login`

Request:
```json
{"api_key":"..."}
```

v1 (200):
```json
{
  "token": "hex.sig",
  "expires_in": 86400,
  "user": {"username":"viewer1","role":"viewer"}
}
```

v2 (200):
```json
{
  "data": {
    "token": "hex.sig",
    "expires_in": 86400,
    "user": {"username":"viewer1","role":"viewer"}
  }
}
```

### `POST /api/auth/refresh` — `POST /api/v2/auth/refresh`

v1:
```json
{"token":"...","expires_in":86400}
```

v2:
```json
{"data":{"token":"...","expires_in":86400}}
```

### `GET /api/health` — `GET /api/v2/health`

v1 (200 / 503):
```json
{"status":"ok","postgres":"ok","redis":"ok"}
```

v2:
```json
{"data":{"status":"ok","postgres":"ok","redis":"ok"}}
```

Error (semua auth):
- v1: `{"error":"..."}`
- v2: `{"error":{"code":"invalid_key","message":"..."}}`

---

## 2) CONTACTS (v1, viewer)

### `GET /api/contacts`

Query: `page`, `per_page≤200`, `domain`, `has_email=true`, `email`, `email_provider`

```json
{
  "data": [
    {
      "id": 123,
      "business_name": "Acme Clinic",
      "business_category": "Wellness",
      "description": "...",
      "website": "https://acme.com",
      "emails": ["hi@acme.com"],
      "phones": ["+62..."],
      "domain": "acme.com",
      "url": "https://acme.com/about",
      "social_links": {"facebook":"..."},
      "address": "...",
      "location": "Jakarta, ID",
      "opening_hours": "09:00-17:00",
      "rating": "4.8",
      "page_title": "About",
      "created_at": "2026-04-20T09:00:00Z"
    }
  ],
  "total": 1234,
  "page": 1,
  "per_page": 50
}
```

### `GET /api/contacts/export?format=json|csv&email_provider=gmail.com`

- `format=json` → streaming array:
  ```json
  [
    {
      "business_name":"...","business_category":"...","website":"...",
      "emails":["..."],"domain":"...","social_links":{...},
      "address":"...","location":"...","phone":"..."
    }
  ]
  ```
- `format=csv` → `text/csv` header:
  ```
  business_name,category,website,emails,domain,social_links,address,location,phone
  ```

### `GET /api/contacts/stats`

```json
{
  "total": 500000,
  "with_email": 210000,
  "unique_domains": 87000,
  "unique_emails": 828000,
  "last_hour": 1200,
  "last_24h": 34000,
  "providers": {"gmail.com":420000,"yahoo.com":90000},
  "validation": {"valid":310000,"pending":420000,"invalid":98000}
}
```

### `DELETE /api/contacts` ❌ admin-only

---

## 3) DOMAINS & CATEGORIES (v1, viewer)

### `GET /api/domains`

```json
[{"domain":"acme.com","emails":12}]
```

### `GET /api/categories`

```json
[{"category":"Wellness","count":5421}]
```

---

## 4) QUERIES (v1)

### `GET /api/queries`

Query: `page`, `per_page≤200`, `status`, `search`

```json
{
  "data": [
    {"id":1,"text":"gym jakarta","status":"completed","result_count":45,"error":"","created_at":"2026-04-20T..."}
  ],
  "total": 1200,
  "page": 1,
  "per_page": 50
}
```

### `GET /api/queries/stats`

```json
{"new":120,"processing":5,"completed":980,"failed":15,"total":1120}
```

### `POST /api/queries`, `DELETE /api/queries`, `POST /api/queries/generate`, `POST /api/queries/retry` ❌ admin-only

---

## 5) PIPELINE & DASHBOARD (v1, viewer)

### `GET /api/pipeline/stats`

```json
{
  "queries_total": 1120,
  "serp_jobs_total": 5700000,
  "enrichment_jobs_total": 900000,
  "emails_total": 828000,
  "business_listings_total": 500000,
  "queues": {
    "serp:queue:queries": 42,
    "serp:buffer": 180,
    "enrich:buffer": 75
  }
}
```

### `GET /api/dashboard`

```json
{
  "queries": {"new":120,"processing":5,"completed":980,"failed":15,"total":1120},
  "serp": {
    "total":5700000,"pending":120,"processing":8,"completed":5600000,"failed":9800,
    "urls_found":12000000,"rate_per_hour":45000,"today":1100000
  },
  "enrich": {
    "total":900000,"pending":4200,"processing":12,"completed":870000,
    "failed":15000,"dead":800,"rate_per_hour":5200,"today":110000
  },
  "contacts": {
    "total_emails":828000,"unique_emails":828000,"emails_today":34000,
    "emails_per_hour":1200,"providers":{"gmail.com":420000},"unique_domains":87000
  },
  "queues": {"serp:queue:queries":42,"serp:buffer":180,"enrich:buffer":75}
}
```

### `POST /api/pipeline/reset` ❌ admin-only

---

## 6) DEBUG (v1, viewer)

### `GET /api/debug/serp-jobs?query_id=<id>&limit=50`

```json
[
  {
    "id":"uuid","parent_job_id":10,"search_url":"https://bing.com/...",
    "page_num":1,"status":"completed","attempt_count":1,"max_attempts":3,
    "error":"","result_count":10,"created_at":"...","updated_at":"..."
  }
]
```

### `GET /api/debug/enrich-jobs?status=pending&limit=50`

```json
[
  {
    "id":"uuid","domain":"acme.com","url":"https://acme.com",
    "status":"completed","attempt_count":1,"max_attempts":3,"error":"",
    "emails":["hi@acme.com"],"phones":["+62..."],
    "created_at":"...","updated_at":"..."
  }
]
```

---

## 7) V2 RESULTS (viewer)

Semua V2 list/single response dibungkus:
- list → `{"data": [...], "meta": {page, per_page, total, total_pages}}`
- single → `{"data": ...}`
- error → `{"error": {"code":"...","message":"..."}}`

### `GET /api/v2/results`

Query: `page`, `per_page≤200`, `domain`, `has_email`, `email`, `email_provider`, `email_status`, `search`

```json
{
  "data": [
    {
      "id": 123,
      "domain": "acme.com",
      "url": "https://acme.com/about",
      "business_name": "Acme Clinic",
      "category": "Wellness",
      "description": "...",
      "address": "...",
      "location": "Jakarta",
      "phone": "+62...",
      "phones": ["+62..."],
      "website": "https://acme.com",
      "page_title": "About",
      "social_links": {"facebook":"..."},
      "opening_hours": "09:00-17:00",
      "rating": "4.8",
      "source_query_id": 42,
      "created_at": "2026-04-20T...",
      "updated_at": "2026-04-20T...",
      "emails": ["hi@acme.com"],
      "emails_with_info": [
        {
          "email":"hi@acme.com","domain":"acme.com","status":"valid",
          "score":0.95,"is_acceptable":true,"mx_valid":true,"deliverable":true,
          "disposable":false,"role_account":false,"free_email":false,"catch_all":false,
          "reason":"","source":"enrichment","validated_at":"2026-04-20T..."
        }
      ],
      "total_email_count": 1,
      "valid_email_count": 1
    }
  ],
  "meta": {"page":1,"per_page":50,"total":500000,"total_pages":10000}
}
```

### `GET /api/v2/results/stats`

```json
{
  "data": {
    "total_listings": 500000,
    "with_email": 210000,
    "with_phone": 95000,
    "unique_domains": 87000,
    "total_emails": 828000,
    "valid_emails": 310000,
    "pending_emails": 420000,
    "invalid_emails": 98000,
    "emails_per_hour": 1200,
    "emails_per_24h": 34000,
    "top_providers": {"gmail.com":420000,"yahoo.com":90000}
  }
}
```

### `GET /api/v2/results/count?<filters>`

```json
{"data":{"count":12345}}
```

### `GET /api/v2/results/categories`

```json
{"data":[{"category":"Wellness","business_count":5421,"email_count":3200}]}
```

### `GET /api/v2/results/domains`

```json
{"data":[{"domain":"acme.com","email_count":12}]}
```

### `GET /api/v2/results/download?format=json|csv&<filters>`

- `json` → streaming array `[V2BusinessListing, ...]` (schema sama dengan item `/api/v2/results`)
- `csv` → header:
  ```
  id,business_name,category,domain,website,address,location,phone,emails,email_count,valid_email_count,created_at
  ```

### `DELETE /api/v2/results` ❌ admin-only

---

## 8) V2 DASHBOARD (viewer)

### `GET /api/v2/stats`

```json
{
  "data": {
    "queries": {"new":120,"processing":5,"completed":980,"failed":15,"total":1120},
    "serp": {
      "total":5700000,"pending":120,"processing":8,"completed":5600000,
      "failed":9800,"urls_found":12000000,"rate_per_hour":45000,"today":1100000
    },
    "enrich": {
      "total":900000,"pending":4200,"processing":12,"completed":870000,
      "failed":15000,"dead":800,"rate_per_hour":5200,"today":110000
    },
    "results": {
      "total_emails":828000,"emails_today":34000,"emails_per_hour":1200,
      "unique_domains":87000,"providers":{"gmail.com":420000}
    },
    "queues": {"serp:queue:queries":42,"serp:buffer":180,"enrich:buffer":75}
  }
}
```

---

## 9) V2 QUERIES (viewer read-only)

### `GET /api/v2/queries`

Query: `page`, `per_page≤200`, `status`, `search`

```json
{
  "data": [
    {
      "id":1,"text":"gym jakarta","status":"completed","result_count":45,
      "error":"","created_at":"2026-04-20T..."
    }
  ],
  "meta": {"page":1,"per_page":50,"total":1200,"total_pages":24}
}
```

### `POST /api/v2/queries`, `DELETE /api/v2/queries`, `POST /api/v2/queries/generate`, `POST /api/v2/queries/retry` ❌ admin-only

---

## Catatan format & error

- **v1**: list `{data,total,page,per_page}`; koleksi langsung sebagai array (`/domains`, `/categories`, `/debug/*`); error `{"error":"message"}`.
- **v2**: list `{data,meta:{page,per_page,total,total_pages}}`; single `{data:...}`; error `{"error":{code,message}}`.
- Viewer yang memanggil admin-only endpoint menerima:
  - v1: `403 {"error":"insufficient permissions"}`
  - v2: `403 {"error":{"code":"...","message":"insufficient permissions"}}`
- Viewer tanpa/berinvalid token: `401 {"error":"missing authorization"}` / `{"error":"invalid token signature"}` / `{"error":"token expired"}`.
