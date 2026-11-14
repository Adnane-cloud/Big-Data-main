// =============================================================================
// donnees_streaming.js — Script de chargement des données (comme shop_maroc.js du TP)
// Utilisation dans mongosh :  load("/scripts/01_initialisation/donnees_streaming.js")
// Relancer le script remet la base à zéro. Les données sont toujours les mêmes
// (tirage pseudo-aléatoire à graine fixe), et datées par rapport au 15/11/2026.
//   2 000 artistes, 10 000 utilisateurs, 20 000 titres, ~150 000 écoutes, 6 000 playlists
// =============================================================================
db = db.getSiblingDB("music_streaming");
db.dropDatabase();

// ---------- tirage pseudo-aléatoire reproductible ----------
let graine = 42;
function hasard() {
  graine = (graine + 0x6D2B79F5) >>> 0;
  let t = graine;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const entre = (min, max) => Math.floor(hasard() * (max - min + 1)) + min;
const choisir = (tab) => tab[Math.floor(hasard() * tab.length)];
function plusieurs(tab, k) {
  const copie = tab.slice(), res = [];
  for (let i = 0; i < k; i++) res.push(copie.splice(Math.floor(hasard() * copie.length), 1)[0]);
  return res;
}
const num = (prefixe, i, n) => prefixe + String(i).padStart(n, "0");
const jourDe = (d) => d.toISOString().slice(0, 10);

// ---------- vocabulaire ----------
const GENRES = ["Rock", "Pop", "Hip-Hop", "Electro", "Jazz", "Metal", "Classique", "R&B", "Reggae", "Blues", "Indie", "Techno", "Soul", "Folk"];
const PAYS = ["MA", "FR", "US", "GB", "DE", "CA", "JP", "BR", "ES", "IT"];
const APPAREILS = ["smartphone", "ordinateur", "tablette", "enceinte_connectee", "tv"];
const SOURCES = ["playlist", "album", "recherche", "radio"];
const LABELS = ["Universal Music", "Sony Music", "Warner Music", "Def Jam", "Roadrunner", "Blue Note", "Indie Records"];
const PRENOMS = ["Adam", "Sara", "Yassine", "Lina", "Omar", "Nora", "Hugo", "Emma", "Leo", "Ines", "Mehdi", "Salma", "Lucas", "Jade", "Karim", "Alice", "Rayan", "Chloe", "Ali", "Maya"];
const NOMS = ["Benali", "Martin", "Alaoui", "Bernard", "Idrissi", "Dubois", "Tazi", "Moreau", "Haddad", "Laurent", "Fassi", "Petit", "Amrani", "Garcia", "Chraibi", "Roux"];
const MOTS = ["Night", "Fire", "Ocean", "Dream", "Shadow", "Light", "Echo", "Storm", "Gold", "Velvet", "Neon", "Desert", "Moon", "River", "Silence", "Wild", "Electric", "Paper", "Glass", "Summer"];
const NOM_PLAYLIST = ["Vibes", "Mix", "Hits", "Party", "Chill", "Session", "Focus", "Road Trip"];
// comportements réalistes
const PROBA_SAUT = { "Pop": 0.30, "Hip-Hop": 0.28, "Electro": 0.26, "Techno": 0.25, "Rock": 0.22, "Metal": 0.20, "R&B": 0.22,
                     "Reggae": 0.18, "Indie": 0.21, "Soul": 0.16, "Folk": 0.15, "Blues": 0.14, "Jazz": 0.12, "Classique": 0.08 };
const BONUS_SAUT = { gratuit: 0.06, famille: 0, premium: -0.04 };
const SESSIONS = { gratuit: [1, 2], famille: [2, 3], premium: [3, 4] };
const FACTEUR_APPAREIL = { smartphone: 0.85, ordinateur: 0.95, tablette: 0.90, enceinte_connectee: 1.0, tv: 0.97 };
const POIDS_HEURE = [4, 2, 2, 1, 1, 2, 4, 8, 10, 8, 6, 6, 8, 8, 6, 6, 8, 10, 12, 14, 16, 16, 12, 8];
const HEURES = [];
POIDS_HEURE.forEach((p, h) => { for (let k = 0; k < p; k++) HEURES.push(h); });

const REFERENCE = new Date("2026-11-15T00:00:00Z");   // « aujourd'hui » des données
const JOUR = 86400000;

// =============================================================================
// 1. ARTISTES — genres (ensemble), membres (liste), reseaux_sociaux (clé-valeur)
// =============================================================================
const artistes = [];
for (let i = 1; i <= 2000; i++) {
  const solo = hasard() < 0.6;
  const nom = solo ? choisir(PRENOMS) + " " + choisir(NOMS) : "The " + choisir(MOTS) + " " + choisir(MOTS) + "s";
  const pseudo = nom.toLowerCase().replace(/ /g, "");
  artistes.push({
    _id: num("art_", i, 5), nom: nom, pays: choisir(PAYS),
    genres: plusieurs(GENRES, entre(1, 3)),
    membres: solo ? [nom] : Array.from({ length: entre(2, 5) }, () => choisir(PRENOMS) + " " + choisir(NOMS)),
    reseaux_sociaux: { instagram: "@" + pseudo, x: "@" + pseudo + "_officiel", youtube: "youtube.com/@" + pseudo }
  });
}
db.artists.insertMany(artistes);

// =============================================================================
// 2. UTILISATEURS — 55 % gratuit, 35 % premium, 10 % famille
// =============================================================================
const users = [];
for (let i = 1; i <= 10000; i++) {
  const r = hasard();
  users.push({
    _id: num("usr_", i, 6),
    pseudo: (choisir(PRENOMS) + choisir(NOMS)).toLowerCase() + entre(10, 99),
    pays: choisir(PAYS), age: entre(15, 65),
    abonnement: r < 0.55 ? "gratuit" : (r < 0.90 ? "premium" : "famille"),
    genres_preferes: plusieurs(GENRES, entre(1, 4)),
    date_inscription: new Date(REFERENCE.getTime() - entre(30, 730) * JOUR),
    appareils: plusieurs(APPAREILS, entre(1, 3))
  });
}
db.users.insertMany(users);

// =============================================================================
// 3. TITRES — album imbriqué (titre, annee, label) ; compteur nb_ecoutes_total
// =============================================================================
const titres = [];
const albums = {};
for (let i = 1; i <= 20000; i++) {
  const a = choisir(artistes);
  if (!albums[a._id]) albums[a._id] = Array.from({ length: entre(1, 4) }, () =>
    ({ titre: choisir(MOTS) + " " + choisir(MOTS), annee: entre(1975, 2026), label: choisir(LABELS) }));
  const album = choisir(albums[a._id]);
  titres.push({
    _id: num("trk_", i, 6), titre: choisir(MOTS) + " " + choisir(MOTS),
    artiste_id: a._id, artiste_nom: a.nom,
    album: { titre: album.titre, annee: album.annee, label: album.label },
    duree: entre(120, 360), genre: choisir(a.genres), bpm: entre(70, 180),
    explicite: hasard() < 0.18, nb_ecoutes_total: 0
  });
}

// =============================================================================
// 4. ÉCOUTES — par sessions (1 à 12 titres enchaînés) sur les 90 jours avant le 15/11/2026.
//    Champs recopiés au moment de l'écoute (artiste_id, genre, pays) et champs de date
//    précalculés (jour, heure) pour les requêtes analytiques. sautee vaut 1 ou 0.
//    Les titres trk_019001 à trk_020000 ne sont jamais écoutés (A23).
// =============================================================================
const enVogue = Array.from({ length: 30 }, () => entre(1, 19000));
const compteur = {};
let lot = [], total = 0;
for (const u of users) {
  const nbSessions = entre(SESSIONS[u.abonnement][0], SESSIONS[u.abonnement][1]);
  for (let s = 0; s < nbSessions; s++) {
    const joursAvant = hasard() < 0.7 ? entre(1, 30) : entre(31, 90);
    let t = REFERENCE.getTime() - joursAvant * JOUR + choisir(HEURES) * 3600000 + entre(0, 3599) * 1000;
    const recente = joursAvant <= 7;
    const appareil = choisir(APPAREILS), source = choisir(SOURCES);
    const nbTitres = entre(1, 12);
    for (let k = 0; k < nbTitres && t < REFERENCE.getTime(); k++) {
      const idx = (recente && hasard() < 0.08) ? choisir(enVogue) : entre(1, 19000);
      const tr = titres[idx - 1];
      const sautee = hasard() < PROBA_SAUT[tr.genre] + BONUS_SAUT[u.abonnement] ? 1 : 0;
      const duree = sautee ? entre(5, 30) : Math.round(entre(Math.floor(tr.duree * 0.6), tr.duree) * FACTEUR_APPAREIL[appareil]);
      const date = new Date(t);
      lot.push({
        utilisateur_id: u._id, titre_id: tr._id, artiste_id: tr.artiste_id, genre: tr.genre, pays: u.pays,
        horodatage: date, jour: jourDe(date), heure: date.getUTCHours(),
        duree_ecoutee: duree, appareil: appareil, source: source, sautee: sautee
      });
      compteur[tr._id] = (compteur[tr._id] || 0) + 1;
      t += (duree + entre(1, 20)) * 1000;
      if (lot.length === 10000) { db.listens.insertMany(lot); total += lot.length; lot = []; }
    }
  }
}
if (lot.length) { db.listens.insertMany(lot); total += lot.length; }
titres.forEach(tr => { tr.nb_ecoutes_total = compteur[tr._id] || 0; });
db.tracks.insertMany(titres);

// =============================================================================
// 5. PLAYLISTS — tableau ordonné de 5 à 35 titres
// =============================================================================
const playlists = [];
for (let i = 1; i <= 6000; i++) {
  const publique = hasard() < 0.75;
  playlists.push({
    _id: num("ply_", i, 6), proprietaire_id: num("usr_", entre(1, 10000), 6),
    nom: choisir(MOTS) + " " + choisir(NOM_PLAYLIST), publique: publique,
    titres: plusieurs(Array.from({ length: 40 }, () => entre(1, 20000)), entre(5, 35))
              .map(n => ({ titre_id: num("trk_", n, 6), ajoute_le: new Date(REFERENCE.getTime() - entre(1, 180) * JOUR) })),
    nb_abonnes: publique ? entre(0, 850) : 0
  });
}
db.playlists.insertMany(playlists);

print("Base music_streaming prête : " + db.artists.countDocuments({}) + " artistes, " + db.users.countDocuments({}) +
      " utilisateurs, " + db.tracks.countDocuments({}) + " titres, " + db.listens.countDocuments({}) + " écoutes, " +
      db.playlists.countDocuments({}) + " playlists.");
