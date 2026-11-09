const DB_NAME = "sportplace";
const sdb = db.getSiblingDB(DB_NAME);
const N_RUN = 3;

// Helper
function median(arr) {
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

// Ambil rantai tahap eksekusi, mis. "FETCH > IXSCAN" atau "COLLSCAN"
function stageChain(winningPlan) {
  let p = winningPlan.queryPlan || winningPlan;   // MongoDB baru (SBE) membungkus di queryPlan
  const names = [];
  while (p) {
    names.push(p.stage);
    p = p.inputStage || (p.inputStages && p.inputStages[0]);
  }
  return names.join(" > ");
}

function runExplain(spec) {
  if (spec.kind === "find") {
    let c = sdb[spec.coll].find(spec.filter);
    if (spec.sort) c = c.sort(spec.sort);
    return c.explain("executionStats");
  }
  return sdb[spec.coll].explain("executionStats").aggregate(spec.pipeline);
}

// Untuk aggregation, statistik cursor ada di stages[0].$cursor
function pickStats(ex) {
  const cur = ex.stages && ex.stages[0] && ex.stages[0].$cursor ? ex.stages[0].$cursor : ex;
  return { planner: cur.queryPlanner, stats: cur.executionStats };
}

function measure(spec) {
  const times = [];
  let last;
  for (let i = 0; i < N_RUN; i++) {
    last = pickStats(runExplain(spec));
    times.push(last.stats.executionTimeMillis);
  }
  return {
    query: spec.label,
    tahap: stageChain(last.planner.winningPlan),
    nReturned: last.stats.nReturned,
    totalKeysExamined: last.stats.totalKeysExamined,
    totalDocsExamined: last.stats.totalDocsExamined,
    waktu_ms_runs: times.join(", "),
    waktu_ms_median: median(times)
  };
}

function dropIfExists(coll, name) {
  try { sdb[coll].dropIndex(name); } catch (e) { /* belum ada, abaikan */ }
}

// 0. Reset: hapus indeks buatan sendiri agar skrip bisa diulang dari nol
dropIfExists("bookings", "idx_bookings_status_created_tagihan");
dropIfExists("bookings", "idx_bookings_items_tanggal_sewa");
dropIfExists("bookings", "idx_bookings_penyewa");
dropIfExists("users", "uniq_users_email");

print("Jumlah dokumen bookings: " + sdb.bookings.countDocuments());

// 1. Tiga query yang diukur
const samplePenyewa = sdb.bookings.findOne({}, { penyewa_id: 1 }).penyewa_id;

// Q1 - filter + sort + range (indeks majemuk ESR)
const q1 = {
  label: "Q1 bookings status=payment, total_tagihan>=200000, sort created_at desc",
  kind: "find",
  coll: "bookings",
  filter: { status: "payment", total_tagihan: { $gte: 200000 } },
  sort: { created_at: -1 }
};

// Q2 - distribusi sewa per bulan
const q2 = {
  label: "Q2 aggregation sewa per bulan (match items.tanggal_sewa, unwind, group)",
  kind: "aggregate",
  coll: "bookings",
  pipeline: [
    { $match: { "items.tanggal_sewa": { $gte: new Date("2026-03-01"), $lt: new Date("2026-04-01") } } },
    { $unwind: "$items" },
    { $match: { "items.tanggal_sewa": { $gte: new Date("2026-03-01"), $lt: new Date("2026-04-01") } } },
    { $group: { _id: { $dateToString: { format: "%Y-%m", date: "$items.tanggal_sewa" } }, jumlah_sewa: { $sum: 1 } } },
    { $sort: { _id: 1 } }
  ]
};

// Q3 - riwayat pemesanan satu penyewa (indeks single-field)
const q3 = {
  label: "Q3 bookings penyewa_id = " + samplePenyewa,
  kind: "find",
  coll: "bookings",
  filter: { penyewa_id: samplePenyewa }
};

const queries = [q1, q2, q3];

// 2. Explain sebelum indeks
print("\n===== EXPLAIN SEBELUM INDEKS =====");
const before = queries.map(measure);
before.forEach(r => printjson(r));

// 3. Buat 4 indeks
print("\n===== MEMBUAT INDEKS =====");

// (1) Majemuk, aturan ESR: Equality (status) -> Sort (created_at) -> Range (total_tagihan)
sdb.bookings.createIndex(
  { status: 1, created_at: -1, total_tagihan: 1 },
  { name: "idx_bookings_status_created_tagihan" }
);

// (2) Multikey: field items adalah array, indeks pada items.tanggal_sewa
sdb.bookings.createIndex(
  { "items.tanggal_sewa": 1 },
  { name: "idx_bookings_items_tanggal_sewa" }
);

// (3) Single-field pada foreign key penyewa
sdb.bookings.createIndex(
  { penyewa_id: 1 },
  { name: "idx_bookings_penyewa" }
);

// (4) Unik: email pengguna
sdb.users.createIndex(
  { email: 1 },
  { unique: true, name: "uniq_users_email" }
);

print("Indeks bookings:");
printjson(sdb.bookings.getIndexes().map(i => i.name));
print("Indeks users:");
printjson(sdb.users.getIndexes().map(i => i.name));

// 4. Explain sesudah indeks
print("\n===== EXPLAIN SESUDAH INDEKS =====");
const after = queries.map(measure);
after.forEach(r => printjson(r));

// 5. Ringkasan perbandingan
print("\n===== RINGKASAN SEBELUM vs SESUDAH =====");
queries.forEach((q, i) => {
  printjson({
    query: q.label,
    tahap: before[i].tahap + "  ->  " + after[i].tahap,
    nReturned: after[i].nReturned,
    totalDocsExamined: before[i].totalDocsExamined + "  ->  " + after[i].totalDocsExamined,
    waktu_ms_median: before[i].waktu_ms_median + "  ->  " + after[i].waktu_ms_median
  });
});

// 6. Bukti indeks unik bekerja: email duplikat harus ditolak (E11000)
print("\n===== UJI INDEKS UNIK (email duplikat) =====");
const contoh = sdb.users.findOne();
delete contoh._id;                       // biarkan MongoDB membuat _id baru
try {
  sdb.users.insertOne(contoh);
  print("PERINGATAN: insert duplikat berhasil, indeks unik tidak bekerja");
} catch (e) {
  print("Ditolak seperti yang diharapkan: " + e.message);
}