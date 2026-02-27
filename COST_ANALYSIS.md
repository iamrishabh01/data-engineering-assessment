# PostgreSQL Backup & Retention: Cost Analysis Report

## 1. Database Context and Growth Projections

The current scenario involves a 100 GB PostgreSQL database. We are seeing a
daily growth rate of **2 GB**, which equates to a monthly growth of **60 GB**.
Using this data, we can project that the database will grow to **160 GB by day 30**
and **280 GB by day 90**.

The overriding goal of this analysis is to establish a plan that satisfies the
30–90‑day retention period while aggressively reducing cloud storage costs.

## 2. Technical Assessment of Backup Strategies

### Strategy 1: Logical Backups (pg_dump) via S3

This strategy leverages the `pg_dump` command‑line utility to dump the entire
database into a script file, which is then compressed with `gzip` and stored in
Amazon S3.

- **Storage Utilization:** Each backup generates a full database dump of
  approximately 20 GB. Over 90 days, the storage needs add up to approximately
  1.8 TB.
- **Cost Analysis:** With S3 Standard storage, this will cost about
  **$34.50 per month**. With S3 Glacier Deep Archive storage, this can be
  lowered to **$1.49 per month**.

- **Restoration:** Recovery is much faster (10 – 30 minutes) since it copies
  raw data blocks instead of executing SQL queries again.
- **Verdict:** This is the standard tool for production SaaS. It enables you to
  restore your database to any particular second, safeguarding against
  accidental data damage.

### Approach 3: AWS EBS Snapshots

This is an infrastructure‑level backup solution that creates snapshots of the
underlying storage volume.

- **Storage Impact:** The first snapshot is a full copy, and the rest are
  incremental block‑level copies.
- **Financials:** Pricing ranges from **$9.50 per month (30 days)** to
  **$18.50 per month (90 days)**.
- **Restoration:** This is the fastest recovery option available, taking only
  **2 – 5 minutes** with "lazy loading."
- **Verdict:** This is very fast and easy to manage, but it promotes
  "vendor lock‑in" to AWS and does not enable the restoration of individual
tables.

## 3. Speed of Recovery Analysis

Speed of recovery is a key metric when every minute of downtime means lost
revenue.

- **The Winner:** Approach 3 (EBS Snapshots)
  - **Estimated Time:** 2 – 5 minutes.
  - **The "Lazy Loading" Benefit:** AWS takes a new EBS snapshot in virtually
    no time. Although data is being loaded from S3 in the background as it is
    accessed, the database can be mounted and started immediately.
  - **Zero Replay:** No decompression or SQL command execution is required.

- **The Runner‑Up:** Approach 2 (WAL Archiving)
  - **Estimated Time:** 10 – 30 minutes.
  - **Point‑in‑Time Discussion:** Although EBS is faster for a "full disk"
    restore, Approach 2 is the fastest for targeted recovery. If you need to
    restore to a particular second (for example, 2:14:05 PM), WAL archiving is
    the only viable solution to "turn back the clock" without sacrificing an
    entire day's worth of data.

### WHICH IS THE FASTEST TO RESTORE?

**Approach 3 (EBS Snapshots) is the fastest overall.** It can bring a volume
back online in minutes and allows immediate attachment thanks to lazy loading.
For single‑second precision restore, WAL archiving still holds the edge.

## 4. Final Recommendation

**Top Choice: Approach 2 (pg_basebackup + WAL Archiving)**

Suggested implementation using enterprise software such as **pgBackRest** or
**WAL‑G**.

For a 100 GB production SaaS database, I recommend the implementation of WAL
archiving. Although the initial process is more complex than a simple script,
in the long run, the benefits to a customer‑facing service are obvious.

## 5. Justification for Recommendation

### I. Critical Recovery Granularity (PITR)

In a SaaS setup, data loss is a reputation killer.

- **The Risk:** Using `pg_dump`, if your backup is at 6:00 AM and a database
  corruption happens at 2:00 PM, you will be left with 8 hours of lost
  customer data.
- **The Solution:** WAL archiving tracks every single modification. You can go
  back to the second before the problem happened, and the loss of data will be
  almost zero.

### II. Superior Cost Efficiency

As your database size expands from 100 GB to 400 GB+, the "Full Backup"
approach will become cost‑prohibitive.

- **Storage Cost Savings:** Since it only needs to store "deltas"
  (modifications) and not full backups every day, this approach is about **40 %* less expensive over a 90‑day retention period than logical dumps.

### III. Business Continuity

- **Minimal Downtime:** A 10‑30 minute restore vs. a 6‑hour `pg_dump` restore
  can mean the difference between a status page update and a catastrophic
  failure of the service.
- **Low Performance Impact:** `pg_dump` has a large read‑lock and CPU impact on
  the production database. WAL archiving is a low overhead, always‑running
  background process that won't affect users.

### IV. Industry Standardization & Proven Reliability

`pgBackRest` and `WAL‑G` are the industry standards. They have been proven in
large‑scale engineering environments and provide functionality such as
integrity checks that a simple script can't.

---

_End of cost analysis report._
