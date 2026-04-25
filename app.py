from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import json, os, datetime, requests, re, io
from bs4 import BeautifulSoup
from collections import defaultdict
import pdfplumber

app = Flask(__name__)
app.secret_key = 'opcvm-ma-2026'
DATA_FILE    = 'data/flux.json'
RAPPORTS_FILE = 'data/rapports.json'
os.makedirs('data', exist_ok=True)

# ── Dossier Excel OPCVM ───────────────────────────────────────────────────
EXCEL_FOLDER = os.environ.get('EXCEL_FOLDER', 'data/excel')
os.makedirs(EXCEL_FOLDER, exist_ok=True)

# ── Cache mémoire ─────────────────────────────────────────────────────────
# Évite de relire les Excel à chaque requête hover.
# Se rafraîchit automatiquement si un fichier est ajouté/modifié.
_holdings_cache      = {}   # { LIBELLE → [{ fonds, gest, poids, ... }] }
_holdings_cache_mtime = {}  # { filename → mtime } — détection de changements

def _get_folder_signature():
    """Retourne un dict {filename: mtime} pour détecter les changements."""
    sig = {}
    try:
        for f in os.listdir(EXCEL_FOLDER):
            if f.lower().endswith(('.xlsx', '.xls')):
                sig[f] = os.path.getmtime(os.path.join(EXCEL_FOLDER, f))
    except Exception:
        pass
    return sig

def _build_holdings_cache():
    """Construit l'index complet en lisant tous les Excel UNE SEULE FOIS."""
    import pandas as pd
    global _holdings_cache, _holdings_cache_mtime

    index = defaultdict(list)
    sig   = _get_folder_signature()

    for fname, mtime in sig.items():
        fpath = os.path.join(EXCEL_FOLDER, fname)
        try:
            df   = pd.read_excel(fpath, sheet_name=0).fillna('')
            row0 = df.iloc[0] if not df.empty else {}
            fonds_name = str(row0.get('OPCVM', fname.replace('.xlsx','')))
            gest       = str(row0.get('Gestionnaire', ''))
            date       = str(row0.get('Date', ''))

            for _, row in df.iterrows():
                libelle    = str(row.get('Libellé', '')).upper().strip()
                code_ligne = str(row.get('Code ligne', '')).upper().strip()
                if not libelle:
                    continue
                entry = {
                    'fonds':        fonds_name,
                    'gestionnaire': gest,
                    'poids':        float(row.get('Poids (%)', 0) or 0),
                    'montant':      float(row.get('Valorisation', 0) or 0),
                    'cours':        float(row.get('Cours', 0) or 0),
                    'qte':          float(row.get('Quantité', 0) or 0),
                    'isin':         code_ligne,
                    'nature':       str(row.get('Nature', '')),
                    'date':         date,
                }
                index[libelle].append(entry)
                if code_ligne and code_ligne.startswith('MA'):
                    index[code_ligne].append(entry)

        except Exception:
            continue

    _holdings_cache       = dict(index)
    _holdings_cache_mtime = sig

def _get_holdings_cache():
    """Retourne le cache, le reconstruit si le dossier a changé."""
    current_sig = _get_folder_signature()
    if current_sig != _holdings_cache_mtime:
        _build_holdings_cache()
    return _holdings_cache

# ═══════════════════════════════════════════════════════════════════════════
# DATA HELPERS
# ═══════════════════════════════════════════════════════════════════════════
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f: return json.load(f)
    return {"flux":[],"opcvm":[],"societes":[
        "Wafa Gestion","CFG Marchés","Upline Capital Management",
        "Attijari Asset Management","BMCE Capital Gestion","CDG Capital Gestion",
        "Valoris Management","Atlas Capital Management","NEMA Capital","Alphavest",
        "CIH Capital Management","Quantum Capital Gestion","STERLING ASSET MANAGEMENT",
        "Ajarinvest","Winéo Gestion"
    ]}

def save_data(d):
    with open(DATA_FILE,'w') as f: json.dump(d,f,indent=2,ensure_ascii=False)

def load_rapports():
    if os.path.exists(RAPPORTS_FILE):
        with open(RAPPORTS_FILE) as f: return json.load(f)
    return []

def save_rapports(r):
    with open(RAPPORTS_FILE,'w') as f: json.dump(r,f,indent=2,ensure_ascii=False)

def cross_flux(flux):
    by_val = defaultdict(lambda:{"A":0,"V":0,"net":0,"ops":[]})
    for f in flux:
        v = f["valeur"]; by_val[v]["ops"].append(f)
        if f["sens"]=="A": by_val[v]["A"]+=f["montant"]
        else:              by_val[v]["V"]+=f["montant"]
    result=[]
    for val,d in by_val.items():
        d["valeur"]=val; d["net"]=d["A"]-d["V"]; result.append(d)
    return sorted(result,key=lambda x:abs(x["net"]),reverse=True)

# ═══════════════════════════════════════════════════════════════════════════
# HOLDING LOOKUP — core feature
# Returns: for a given ticker/valeur, which fonds hold it and at what weight
# ═══════════════════════════════════════════════════════════════════════════
def build_holding_index():
    """
    Build an index: { 'ATTIJARIWAFA BANK': [
        { fonds, gestionnaire, poids, montant, qte, isin, rapport_id, date },
        ...
    ], ... }
    Matches on designation (normalized) and also on ticker aliases.
    """
    rapports = load_rapports()
    index = defaultdict(list)
    for r in rapports:
        for ligne in r.get('lignes', []):
            val  = ligne.get('valeur','').upper().strip()
            isin = ligne.get('isin','').strip()
            if not val: continue
            entry = {
                'fonds':       r.get('opcvm_name', r.get('source','')),
                'gestionnaire':r.get('gestionnaire', r.get('soc_gestion_override','')),
                'poids':       ligne.get('poids', 0),
                'montant':     ligne.get('montant', 0),
                'qte':         ligne.get('qte', 0),
                'isin':        isin,
                'rapport_id':  r.get('id',''),
                'date':        r.get('date',''),
            }
            index[val].append(entry)
            # Also index by ISIN
            if isin:
                index[isin].append(entry)
    return dict(index)

def search_holdings(query):
    """
    Search the holding index for a query (partial match on name or ISIN).
    Returns list of matching { valeur, holders: [...] }
    """
    query = query.upper().strip()
    index = build_holding_index()
    results = {}
    for key, holders in index.items():
        if query in key or key in query:
            # Use designation if key is ISIN
            display = key
            results[display] = holders
    return results

# ═══════════════════════════════════════════════════════════════════════════
# PDF PARSER — fixed for ISIN+Désignation format (TCB11 Annexe)
# ═══════════════════════════════════════════════════════════════════════════
ISIN_RE  = re.compile(r'^MA\d{10}$')
SKIP_KW  = {
    'total','sous-total','sous total','total actions','total obligations',
    'total tct','total bons','actif net','valeur liquidative','liquidative',
    'valeur','quantite','quantité','cours','montant','actif','libelle','libellé',
    'designation','désignation','titres','nombre','description','actions cotées',
    'actions cotees','obligations','titres de créances','opcvm','fcp','sicav',
    'total portefeuille','total général','total general','taux fixes',
    'taux variables','bons du trésor','tresor','sous-total actions',
    'code isin','désignation','quantité','valeur actuelle','% actif immo',
    '% total actif','% actif','inventaire','hors actif','actif immobilier',
    'annexe','dépositaire','depositaire','gestionnaire','data inventaire',
}

def is_isin(s):
    return bool(ISIN_RE.match(str(s).strip().replace(' ','')))

def clean_num(s):
    """Parse French-format numbers: spaces as thousands sep, comma as decimal."""
    if not s: return 0.0
    s = str(s).strip()
    s = s.replace('\xa0','').replace('\u202f','').replace('\u00a0','')
    s = s.replace('%','').strip()
    # Remove space thousands separators, normalize decimal comma → dot
    s = s.replace(' ','').replace(',','.')
    s = s.rstrip('.')
    m = re.match(r'^-?([\d]+\.?[\d]*)$', s)
    if m:
        try: return float(m.group())
        except: return 0.0
    return 0.0

def is_pct_cell(s):
    return '%' in str(s) if s else False

def parse_opcvm_pdf_bytes(pdf_bytes, source_name='', soc_gestion=''):
    """
    Word-position based parser — works on borderless financial PDFs (TCB11 format).
    Groups words by Y-position into rows, detects ISIN column, reconstructs numbers.
    """
    result = {
        'opcvm_name': source_name, 'gestionnaire': soc_gestion,
        'date': '', 'actif_net': 0, 'lignes': [], 'raw_page_count': 0,
        'parsed_at': datetime.datetime.now().isoformat(), 'source': source_name
    }
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            result['raw_page_count'] = len(pdf.pages)
            full_text = ''
            for page in pdf.pages:
                full_text += (page.extract_text() or '') + '\n'

            # ── Metadata ────────────────────────────────────────────────
            for pattern, key in [
                (r'OPCVM\s*:\s*(?:FCP|SICAV|FPI|OPCI)?\s*([^\n\r]{3,60}?)(?:\s{3,}|\s*Data\s|\s*Depot|\s*Dép|\s*$)', 'opcvm_name'),
                (r'(?:FCP|SICAV|OPCI)\s+([A-Z][A-Z\s\-\d\.]{3,50}?)(?:\s{2,}|\n)', 'opcvm_name'),
                (r'(?:GESTIONNAIRE|GERANT|SOC\.?\s*DE\s*GESTION)\s*:\s*([^\n\r]{3,50}?)(?:\s{3,}|\s*Dep|\s*$)', 'gestionnaire'),
                (r'(?:DATA INVENTAIRE|INVENTAIRE DU|AU|CLOS LE|PERIODE AU)\s*[:\s]*(\d{2}[/.-]\d{2}[/.-]\d{4})', 'date'),
                (r'(\d{2}[/.-]\d{2}[/.-]\d{4})', 'date'),
            ]:
                if result[key]: continue
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m: result[key] = m.group(1).strip().strip('«»""').strip()

            # ── Actif Net / Total Actif ──────────────────────────────────
            for p in [
                r'(?:TOTAL ACTIF|ACTIF NET GLOBAL|ACTIF NET)\s*[:\s]*([\d\s\xa0\u202f,\.]{5,30}?)(?:\s*\d{1,3}[,%]|\s*$)',
                r'NET ASSET VALUE\s*[:\s]*([\d\s,\.]{5,30})',
            ]:
                m = re.search(p, full_text, re.IGNORECASE)
                if m:
                    raw = re.sub(r'\s', '', m.group(1))[:28]
                    v = clean_num(raw)
                    if v > result['actif_net']: result['actif_net'] = v

            # ── Word-position row reconstruction ─────────────────────────
            SKIP_ROWS = {
                'total','sous-total','total actif','actif net','code isin',
                'désignation','designation','libellé','libelle','quantité','quantite',
                'valeur actuelle','valeur glob','annexe','inventaire','opcvm',
                'gestionnaire','depositaire','dépositaire','% actif','% total',
                'hors actif','actif immobilier','data inventaire','titres','nombre',
                'cours','montant','obligations','tct','bons du trésor','actions cotées',
                'total obligations','total actions','total tct','total bons',
            }

            lignes_seen = set()

            for page in pdf.pages:
                words = page.extract_words(
                    keep_blank_chars=False,
                    x_tolerance=2, y_tolerance=2,
                    extra_attrs=['fontname','size']
                )
                if not words: continue

                # Group words into rows by Y position (3pt bucket)
                rows_dict = {}
                for w in words:
                    y_key = round(w['top'] / 3) * 3
                    rows_dict.setdefault(y_key, []).append(w)

                sorted_rows = sorted(rows_dict.items())

                # Detect header row → find x-positions of each column
                col_x = {}  # 'isin','desig','qte','val','pct1','pct2'
                for y_key, rw in sorted_rows:
                    texts = ' '.join(w['text'].lower() for w in rw)
                    if ('isin' in texts or 'désig' in texts or 'libellé' in texts) and \
                       any(k in texts for k in ['quantit','qté','nbre','valeur','montant']):
                        # Merge adjacent header words to handle "Valeur Actuelle", "% Actif Immo"
                        merged = []
                        prev = None
                        for w in sorted(rw, key=lambda x: x['x0']):
                            if prev and (w['x0'] - prev['x1']) < 12:
                                prev = {**prev, 'text': prev['text']+' '+w['text'], 'x1': w['x1']}
                            else:
                                if prev: merged.append(prev)
                                prev = dict(w)
                        if prev: merged.append(prev)

                        for w in merged:
                            t = w['text'].lower()
                            if 'isin' in t:                                                    col_x['isin']  = w['x0']
                            elif any(k in t for k in ['désig','design','libellé','libelle']):  col_x['desig'] = w['x0']
                            elif any(k in t for k in ['quantit','qté','nbre','nombre']):       col_x['qte']   = w['x0']
                            elif any(k in t for k in ['valeur actuelle','valeur glob','montant','cours']): col_x['val'] = w['x0']
                            elif '%' in t and 'immo' in t:                                     col_x['pct1']  = w['x0']
                            elif '%' in t and 'total' in t:                                    col_x['pct2']  = w['x0']
                            elif '%' in t and 'pct1' not in col_x:                            col_x['pct1']  = w['x0']
                        break

                for y_key, rw in sorted_rows:
                    rw_sorted = sorted(rw, key=lambda w: w['x0'])
                    first = rw_sorted[0]['text'].strip()

                    # Must start with ISIN
                    if not ISIN_RE.match(first.replace(' ', '')): continue
                    isin = first.replace(' ', '')

                    # Separate designation words from numeric words
                    desig_words, num_words = [], []
                    for w in rw_sorted[1:]:
                        t = w['text'].strip()
                        if not t: continue
                        # Pure numeric token (digits, spaces, comma/dot as decimal, %)
                        clean = t.replace(' ','').replace('\xa0','').replace(',','.').replace('%','')
                        if re.match(r'^\d[\d\.]*$', clean):
                            num_words.append(w)
                        elif t in (',', '.', '%'):
                            num_words.append(w)
                        else:
                            desig_words.append(w)

                    desig = ' '.join(w['text'] for w in sorted(desig_words, key=lambda w: w['x0'])).strip()
                    if not desig: continue
                    key_d = desig.upper().strip()
                    if any(sk in key_d.lower() for sk in SKIP_ROWS): continue
                    if key_d[:30] in lignes_seen: continue

                    # Reconstruct multi-word numbers (e.g. "3 671 000")
                    # Words belonging to same number are within 8pt horizontally
                    num_groups = []
                    cur = []
                    for nw in sorted(num_words, key=lambda w: w['x0']):
                        if cur and (nw['x0'] - cur[-1]['x1']) > 9:
                            num_groups.append(cur); cur = [nw]
                        else:
                            cur.append(nw)
                    if cur: num_groups.append(cur)

                    num_strs = [''.join(w['text'] for w in g) for g in num_groups]
                    num_vals = []
                    for s in num_strs:
                        v = clean_num(s)
                        if v > 0: num_vals.append((v, is_pct_cell(s), num_groups[num_strs.index(s)][0]['x0']))

                    if not num_vals: continue

                    ligne = {'valeur': key_d, 'isin': isin, 'qte': 0., 'montant': 0., 'poids': 0.}

                    # If we know column positions, assign by x proximity
                    if col_x:
                        for v, is_p, x in num_vals:
                            dists = {k: abs(x - cx) for k, cx in col_x.items() if k not in ('isin','desig')}
                            if not dists: continue
                            closest = min(dists, key=dists.get)
                            if   closest == 'qte':  ligne['qte']     = v
                            elif closest == 'val':  ligne['montant'] = v   # valeur actuelle
                            elif closest == 'pct1': ligne['poids']   = v
                            elif closest == 'pct2' and ligne['poids'] == 0: ligne['poids'] = v
                    else:
                        # Fallback: largest = montant (valeur actuelle), second = qte, pct-tagged = poids
                        pcts   = [(v,x) for v,p,x in num_vals if p]
                        plains = sorted([(v,x) for v,p,x in num_vals if not p], reverse=True)
                        if pcts:   ligne['poids']   = pcts[0][0]
                        if plains:
                            ligne['montant'] = plains[0][0]   # largest = valeur actuelle
                        if len(plains) >= 2:
                            ligne['qte'] = plains[1][0]       # second = quantité

                    if ligne['montant'] > 0 or ligne['qte'] > 0:
                        lignes_seen.add(key_d[:30])
                        result['lignes'].append(ligne)

            # Fallback to text-line parser if words approach yielded nothing
            if not result['lignes']:
                result['lignes'] = _text_fallback(full_text)

            # ── SCANNED PDF DETECTION ────────────────────────────────────
            # If still no lines AND total words = 0 across all pages → scanned PDF
            total_words = sum(
                len(page.extract_words()) for page in pdf.pages
            )
            if not result['lignes'] and total_words == 0:
                result['_is_scanned'] = True

    except Exception as e:
        result['error'] = str(e)

    # If scanned PDF → route to OCR parser
    if result.get('_is_scanned'):
        ocr_result = parse_scanned_pdf(pdf_bytes, source_name, soc_gestion)
        # Merge: keep metadata from whichever has more info
        for key in ['opcvm_name','gestionnaire','date','actif_net','lignes']:
            if ocr_result.get(key): result[key] = ocr_result[key]
        result['ocr_used'] = True
        result.pop('_is_scanned', None)
        return result

    # Compute missing poids from actif net
    if result['actif_net'] > 0:
        for l in result['lignes']:
            if l['poids'] == 0 and l['montant'] > 0:
                l['poids'] = round(l['montant'] / result['actif_net'] * 100, 4)

    return result


def _text_fallback(text):
    lignes = []
    pat = re.compile(
        r'^([A-Z][A-Z\s\-\.&\(\)\']{2,50}?)\s+([\d\s]{1,20})\s+([\d\s,\.]{1,15})\s+([\d\s,\.]{3,20})\s*([\d,\.]+\s*%?)?\s*$',
        re.MULTILINE
    )
    for m in pat.finditer(text.upper()):
        name = m.group(1).strip()
        if any(kw in name.lower() for kw in SKIP_KW): continue
        l = {'valeur':name, 'isin':'', 'qte':clean_num(m.group(2)), 'cours':clean_num(m.group(3)),
             'montant':clean_num(m.group(4)), 'poids':clean_num(m.group(5)) if m.group(5) else 0}
        if l['montant'] > 0: lignes.append(l)
    return lignes


def scrape_communiques_listing(societe=None):
    try:
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                   'Accept-Language':'fr-FR,fr;q=0.9','Referer':'https://www.google.com/'}
        r = requests.get('https://medias24.com/communications/', headers=headers, timeout=10)
        if r.status_code == 403:
            return [], "Medias24 bloque les requêtes automatiques (403). Ouvrez le site dans votre navigateur, téléchargez le PDF et importez-le ci-dessous."
        soup = BeautifulSoup(r.text, 'html.parser')
        items = []
        for a in soup.select("a[href*='financiere_com']"):
            title = a.get_text(strip=True); href = a.get('href','')
            if not title or len(title) < 10: continue
            if societe and societe.lower() not in title.lower(): continue
            is_r = any(k in title.lower() for k in ['commissaire','rapport','bilan','portefeuille','composition','actif','liquidative','inventaire'])
            items.append({'titre':title,'lien':href,'is_rapport':is_r})
        return items[:40], None
    except Exception as e:
        return [], str(e)


# ═══════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    data = load_data(); cross = cross_flux(data['flux'])
    total_a = sum(f['montant'] for f in data['flux'] if f['sens']=='A')
    total_v = sum(f['montant'] for f in data['flux'] if f['sens']=='V')
    return render_template('index.html',
        flux=data['flux'][-20:][::-1], cross=cross[:10], opcvm=data['opcvm'],
        societes=data['societes'], total_a=total_a, total_v=total_v,
        nb_flux=len(data['flux']), nb_valeurs=len(cross))

@app.route('/flux/add', methods=['POST'])
def add_flux():
    data = load_data()
    f = {'id':len(data['flux'])+1, 'date':request.form.get('date',str(datetime.date.today())),
         'valeur':request.form.get('valeur','').upper().strip(), 'sens':request.form.get('sens','A'),
         'qte':float(request.form.get('qte',0) or 0), 'prix':float(request.form.get('prix',0) or 0),
         'montant':float(request.form.get('montant',0) or 0), 'opcvm':request.form.get('opcvm',''),
         'soc_gestion':request.form.get('soc_gestion',''), 'note':request.form.get('note',''),
         'ts':datetime.datetime.now().isoformat()}
    if f['montant']==0 and f['qte'] and f['prix']: f['montant']=f['qte']*f['prix']
    data['flux'].append(f); save_data(data)
    return redirect(url_for('index'))

@app.route('/flux/delete/<int:fid>', methods=['POST'])
def delete_flux(fid):
    data = load_data(); data['flux']=[f for f in data['flux'] if f.get('id')!=fid]; save_data(data)
    return redirect(url_for('index'))

@app.route('/opcvm/add', methods=['POST'])
def add_opcvm():
    data = load_data()
    o = {'id':len(data['opcvm'])+1, 'nom':request.form.get('nom','').strip(),
         'valeur':request.form.get('valeur','').upper().strip(),
         'qte':float(request.form.get('qte',0) or 0), 'cmp':float(request.form.get('cmp',0) or 0),
         'prix_marche':float(request.form.get('prix_marche',0) or 0),
         'fonds':request.form.get('fonds',''), 'soc_gestion':request.form.get('soc_gestion',''),
         'date_maj':str(datetime.date.today())}
    o['valeur_position']=o['qte']*o['prix_marche']; o['pnl']=(o['prix_marche']-o['cmp'])*o['qte']
    data['opcvm'].append(o); save_data(data)
    return redirect(url_for('inventaire'))

@app.route('/inventaire')
def inventaire():
    """Liste de tous les fonds depuis le dossier Excel."""
    fonds_list = _load_all_excel_fonds()
    data = load_data()
    return render_template('inventaire.html',
        fonds_list=fonds_list,
        societes=data['societes'],
        nb_fonds=len(fonds_list))


@app.route('/inventaire/<fid>')
def inventaire_fonds(fid):
    """Détail d'un fonds depuis son fichier Excel (fid = nom fichier sans .xlsx)."""
    import urllib.parse
    filename = urllib.parse.unquote(fid) + '.xlsx'
    filepath = os.path.join(EXCEL_FOLDER, filename)

    if not os.path.exists(filepath):
        flash(f'Fichier introuvable : {filename}', 'error')
        return redirect(url_for('inventaire'))

    fonds = _read_excel_fonds(filepath)
    return render_template('inventaire_fonds.html', fonds=fonds)


@app.route('/inventaire/upload', methods=['POST'])
def inventaire_upload():
    """Upload d'un ou plusieurs fichiers Excel dans le dossier."""
    files = request.files.getlist('excel_files')
    if not files:
        flash('Aucun fichier sélectionné', 'error')
        return redirect(url_for('inventaire'))
    saved = 0
    for f in files:
        if f.filename.lower().endswith(('.xlsx', '.xls')):
            f.save(os.path.join(EXCEL_FOLDER, f.filename))
            saved += 1
    flash(f'✓ {saved} fichier(s) importé(s)', 'success')
    return redirect(url_for('inventaire'))


# ── Helpers Excel ─────────────────────────────────────────────────────────
def _load_all_excel_fonds():
    """
    Construit la liste résumée des fonds depuis le cache.
    Pas de lecture disque si le cache est à jour.
    """
    import urllib.parse
    idx = _get_holdings_cache()   # garantit que le cache est frais

    # Regrouper les entrées par fonds
    fonds_map = {}
    for libelle, entries in idx.items():
        for e in entries:
            key = e['fonds']
            if key not in fonds_map:
                fonds_map[key] = {
                    'nom':          e['fonds'],
                    'gestionnaire': e['gestionnaire'],
                    'date':         e['date'],
                    'nb_lignes':    0,
                    'total_poids':  0.0,
                    # On complétera classification/isin_opcvm/actif_net depuis le fichier
                }
            fonds_map[key]['nb_lignes']   += 1
            fonds_map[key]['total_poids'] += e.get('poids', 0)

    # Ajouter les métadonnées (classification, isin, actif_net) depuis la 1re ligne de chaque Excel
    # Ces données ne sont PAS dans le cache index, on les lit une seule fois par fichier
    # mais seulement la 1ère ligne (nrows=1) → très rapide
    _meta_cache = getattr(_load_all_excel_fonds, '_meta', {})
    current_sig = _get_folder_signature()
    if getattr(_load_all_excel_fonds, '_meta_sig', {}) != current_sig:
        import pandas as pd
        _meta_cache = {}
        for fname in os.listdir(EXCEL_FOLDER):
            if not fname.lower().endswith(('.xlsx', '.xls')):
                continue
            try:
                df   = pd.read_excel(os.path.join(EXCEL_FOLDER, fname), sheet_name=0, nrows=1)
                row0 = df.iloc[0] if not df.empty else {}
                fonds_nom = str(row0.get('OPCVM', fname.replace('.xlsx','')))
                _meta_cache[fonds_nom] = {
                    'classification': str(row0.get('Classification', '')),
                    'isin_opcvm':     str(row0.get('ISIN OPCVM', '')),
                    'actif_net':      float(row0.get('Valo globale OPCVM', 0) or 0),
                    'filename':       fname,
                    'id':             urllib.parse.quote(fname.replace('.xlsx','').replace('.xls','')),
                }
            except Exception:
                pass
        _load_all_excel_fonds._meta     = _meta_cache
        _load_all_excel_fonds._meta_sig = current_sig

    fonds_list = []
    for nom, f in fonds_map.items():
        meta = _meta_cache.get(nom, {})
        fonds_list.append({
            'id':             meta.get('id', urllib.parse.quote(nom)),
            'filename':       meta.get('filename', ''),
            'nom':            nom,
            'gestionnaire':   f['gestionnaire'],
            'date':           f['date'],
            'classification': meta.get('classification', ''),
            'isin_opcvm':     meta.get('isin_opcvm', ''),
            'actif_net':      meta.get('actif_net', 0),
            'nb_lignes':      f['nb_lignes'] // 2,  # index duplique par ISIN → diviser
            'total_poids':    round(f['total_poids'] / 2, 2),
        })

    fonds_list.sort(key=lambda x: x['nom'])
    return fonds_list


def _read_excel_fonds(filepath):
    """
    Retourne les lignes d'un fonds depuis le cache (si disponible)
    ou lit le fichier directement en dernier recours.
    """
    import urllib.parse
    fname    = os.path.basename(filepath)
    fname_no = fname.replace('.xlsx','').replace('.xls','')
    idx      = _get_holdings_cache()

    # Identifier le nom OPCVM de ce fichier
    target_fonds = None
    meta = getattr(_load_all_excel_fonds, '_meta', {})
    for nom, m in meta.items():
        if m.get('filename') == fname:
            target_fonds = nom
            break

    if target_fonds is None:
        # Fallback : lire directement
        import pandas as pd
        df   = pd.read_excel(filepath, sheet_name=0).fillna('')
        row0 = df.iloc[0] if not df.empty else {}
        target_fonds = str(row0.get('OPCVM', fname_no))

    # Construire les lignes depuis le cache (sans relire le disque)
    items_map = {}
    for libelle, entries in idx.items():
        for e in entries:
            if e['fonds'] != target_fonds:
                continue
            # Dédupliquer : garder une seule entrée par code_ligne
            isin = e.get('isin','')
            key  = isin if isin else libelle
            if key not in items_map:
                items_map[key] = {
                    'code_ligne':   isin,
                    'libelle':      libelle,
                    'nature':       e.get('nature', ''),
                    'cours':        e.get('cours', 0),
                    'quantite':     e.get('qte', 0),
                    'valorisation': e.get('montant', 0),
                    'poids':        e.get('poids', 0),
                }

    items = sorted(items_map.values(), key=lambda x: x['poids'], reverse=True)

    meta_f = meta.get(target_fonds, {})
    return {
        'fonds':              target_fonds,
        'gestionnaire':       items[0]['code_ligne'] if False else
                              next((e['gestionnaire'] for entries in idx.values()
                                    for e in entries if e['fonds'] == target_fonds), ''),
        'date':               next((e['date'] for entries in idx.values()
                                    for e in entries if e['fonds'] == target_fonds), ''),
        'classification':     meta_f.get('classification', ''),
        'isin_opcvm':         meta_f.get('isin_opcvm', ''),
        'valo_globale_opcvm': meta_f.get('actif_net', 0),
        'lignes':             items,
        'items':              items,
    }

@app.route('/cross')
def cross_view():
    data = load_data(); cross = cross_flux(data['flux'])
    var_an = {}
    for f in data['flux']:
        v=f['valeur']
        if v not in var_an: var_an[v]={'souscription':0,'rachat':0}
        if f['sens']=='A': var_an[v]['souscription']+=f['montant']
        else: var_an[v]['rachat']+=f['montant']
    var_an_list=[{'valeur':k,**v,'net':v['souscription']-v['rachat']} for k,v in var_an.items()]
    var_an_list.sort(key=lambda x:abs(x['net']),reverse=True)
    return render_template('cross.html', cross=cross, var_an=var_an_list[:20], societes=data['societes'])

@app.route('/rapports')
def rapports():
    data = load_data(); rpts = load_rapports()
    communiques, scrape_error = scrape_communiques_listing(request.args.get('societe',''))
    return render_template('rapports.html', rapports=rpts, communiques=communiques,
        scrape_error=scrape_error, societes=data['societes'], selected_soc=request.args.get('societe',''))

@app.route('/rapports/upload', methods=['POST'])
def upload_rapport():
    if 'pdf_file' not in request.files: flash('Aucun fichier','error'); return redirect(url_for('rapports'))
    f = request.files['pdf_file']
    if not f.filename.lower().endswith('.pdf'): flash('PDF requis','error'); return redirect(url_for('rapports'))
    result = parse_opcvm_pdf_bytes(f.read(),
        source_name=request.form.get('nom_fonds','').strip() or f.filename,
        soc_gestion=request.form.get('soc_gestion',''))
    result['id'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    result['filename'] = f.filename
    flash((f'✓ {len(result["lignes"])} lignes extraites — {result["opcvm_name"]}') if result['lignes'] else '⚠ Aucune ligne — PDF scanné?', 'success' if result['lignes'] else 'warning')
    rpts = load_rapports(); rpts.insert(0, result); save_rapports(rpts)
    return redirect(url_for('rapport_detail', rid=result['id']))

@app.route('/rapports/url', methods=['POST'])
def parse_from_url():
    pdf_url = request.form.get('pdf_url','').strip()
    if not pdf_url: flash('URL vide','error'); return redirect(url_for('rapports'))
    try:
        r = requests.get(pdf_url, headers={'User-Agent':'Mozilla/5.0'}, timeout=20)
        if r.status_code != 200: flash(f'HTTP {r.status_code}','error'); return redirect(url_for('rapports'))
        result = parse_opcvm_pdf_bytes(r.content,
            source_name=request.form.get('nom_fonds','').strip() or pdf_url.split('/')[-1],
            soc_gestion=request.form.get('soc_gestion',''))
        result['id'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S'); result['filename']=pdf_url
        flash(f'✓ {len(result["lignes"])} lignes extraites','success')
        rpts=load_rapports(); rpts.insert(0,result); save_rapports(rpts)
        return redirect(url_for('rapport_detail', rid=result['id']))
    except Exception as e:
        flash(str(e),'error'); return redirect(url_for('rapports'))

@app.route('/rapports/<rid>')
def rapport_detail(rid):
    rpts=load_rapports(); rapport=next((r for r in rpts if r['id']==rid),None)
    if not rapport: flash('Introuvable','error'); return redirect(url_for('rapports'))
    data=load_data()
    flux_by_val=defaultdict(lambda:{'A':0,'V':0})
    for f in data['flux']: flux_by_val[f['valeur']][f['sens']]+=f['montant']
    for ligne in rapport.get('lignes',[]):
        matched=next((fv for fv in flux_by_val if fv in ligne['valeur'] or ligne['valeur'][:8] in fv),None)
        if matched: ligne['flux_A']=flux_by_val[matched]['A']; ligne['flux_V']=flux_by_val[matched]['V']; ligne['flux_net']=ligne['flux_A']-ligne['flux_V']
        else: ligne['flux_A']=ligne['flux_V']=ligne['flux_net']=0
    return render_template('rapport_detail.html', rapport=rapport)

@app.route('/rapports/delete/<rid>', methods=['POST'])
def delete_rapport(rid):
    rpts=load_rapports(); rpts=[r for r in rpts if r['id']!=rid]; save_rapports(rpts)
    return redirect(url_for('rapports'))

# ── API ──────────────────────────────────────────────────────────────────
@app.route('/api/cross')
def api_cross(): return jsonify(cross_flux(load_data()['flux']))

@app.route('/api/flux_chart')
def api_flux_chart():
    flux=load_data()['flux']; by_date=defaultdict(lambda:{'A':0,'V':0})
    for f in flux: by_date[f['date']][f['sens']]+=f['montant']
    dates=sorted(by_date.keys())
    return jsonify({'labels':dates,'achats':[by_date[d]['A'] for d in dates],'ventes':[by_date[d]['V'] for d in dates]})

@app.route('/api/holders/<valeur>')
def api_holders(valeur):
    """Cherche dans le cache qui détient cette valeur."""
    query   = valeur.upper().strip()
    idx     = _get_holdings_cache()
    results = []
    seen    = set()

    for key, entries in idx.items():
        match = (
            (query in key) or
            (key in query) or
            (len(query) >= 4 and query[:4] in key[:min(len(key), 20)])
        )
        if not match:
            continue
        for e in entries:
            uid = f"{e['fonds']}:{key}"
            if uid in seen:
                continue
            seen.add(uid)
            results.append({**e, 'valeur': key})

    results.sort(key=lambda x: x['poids'], reverse=True)
    return jsonify({'valeur': query, 'nb_fonds': len(results), 'holders': results})


@app.route('/api/holders_all')
def api_holders_all():
    """Retourne l'index complet depuis le cache."""
    return jsonify(_get_holdings_cache())

@app.route('/api/rapport/<rid>/composition')
def api_rapport_composition(rid):
    rpts=load_rapports(); r=next((x for x in rpts if x['id']==rid),None)
    if not r: return jsonify({'error':'not found'}),404
    return jsonify(r.get('lignes',[]))

@app.route('/api/opcvm_perf')
def api_opcvm_perf():
    return jsonify([{'nom':o.get('valeur','?'),'pnl':o.get('pnl',0),'val':o.get('valeur_position',0)} for o in load_data()['opcvm']])


# ═══════════════════════════════════════════════════════════════════════════
# CO-DÉTENTION — Top valeurs détenues par le plus de fonds
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/codetention')
def codetention():
    idx = _get_holdings_cache()

    rows = []
    for libelle, entries in idx.items():
        # Dédupliquer par fonds
        fonds_set  = {}
        for e in entries:
            k = e['fonds']
            if k not in fonds_set:
                fonds_set[k] = e
        if len(fonds_set) < 2:
            continue
        total_valo  = sum(e.get('montant', 0) for e in fonds_set.values())
        natures     = list({e.get('nature','') for e in fonds_set.values() if e.get('nature')})
        rows.append({
            'libelle':    libelle,
            'nb_fonds':   len(fonds_set),
            'valo_total': total_valo,
            'nature':     natures[0] if natures else '—',
            'fonds':      sorted(fonds_set.keys()),
        })

    # Top 30 par nb_fonds et par valorisation
    top_nb   = sorted(rows, key=lambda x: x['nb_fonds'],   reverse=True)[:30]
    top_valo = sorted(rows, key=lambda x: x['valo_total'], reverse=True)[:30]
    natures  = sorted({r['nature'] for r in rows if r['nature'] != '—'})

    return render_template('codetention.html',
        top_nb=top_nb, top_valo=top_valo, natures=natures,
        total_valeurs=len(rows))


# ═══════════════════════════════════════════════════════════════════════════
# RECHERCHE VALEUR — chercher un titre et voir tous ses détenteurs
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/recherche')
def recherche_valeur():
    return render_template('recherche_valeur.html')

@app.route('/api/recherche')
def api_recherche():
    q   = request.args.get('q', '').upper().strip()
    if len(q) < 2:
        return jsonify([])
    idx = _get_holdings_cache()
    results = []
    seen = set()
    for libelle, entries in idx.items():
        if q not in libelle:
            continue
        fonds_set = {}
        for e in entries:
            k = e['fonds']
            if k not in fonds_set:
                fonds_set[k] = e
        key = libelle
        if key in seen: continue
        seen.add(key)
        total_valo = sum(e.get('montant',0) for e in fonds_set.values())
        natures = list({e.get('nature','') for e in fonds_set.values() if e.get('nature')})
        results.append({
            'libelle':   libelle,
            'nb_fonds':  len(fonds_set),
            'valo_total':total_valo,
            'nature':    natures[0] if natures else '—',
            'detenteurs': [{
                'fonds':        e['fonds'],
                'gestionnaire': e['gestionnaire'],
                'poids':        e.get('poids',0),
                'montant':      e.get('montant',0),
                'cours':        e.get('cours',0),
                'date':         e.get('date',''),
            } for e in sorted(fonds_set.values(), key=lambda x: x.get('poids',0), reverse=True)]
        })
    results.sort(key=lambda x: x['nb_fonds'], reverse=True)
    return jsonify(results[:50])


# ═══════════════════════════════════════════════════════════════════════════
# PARIS ACTIFS — comparer deux fonds côte à côte
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/paris-actifs')
def paris_actifs():
    fonds_list = _load_all_excel_fonds()
    return render_template('paris_actifs.html', fonds_list=fonds_list)

@app.route('/api/paris-actifs')
def api_paris_actifs():
    fa = request.args.get('a','')
    fb = request.args.get('b','')
    nature_filter = request.args.get('nature','').upper()
    import urllib.parse

    def get_fonds_lignes(fid):
        fname = urllib.parse.unquote(fid) + '.xlsx'
        fpath = os.path.join(EXCEL_FOLDER, fname)
        if not os.path.exists(fpath): return []
        f = _read_excel_fonds(fpath)
        return f.get('lignes', [])

    lignes_a = {l['code_ligne']: l for l in get_fonds_lignes(fa) if l.get('code_ligne')}
    lignes_b = {l['code_ligne']: l for l in get_fonds_lignes(fb) if l.get('code_ligne')}

    all_codes = set(lignes_a) | set(lignes_b)
    communs = all_codes & set(lignes_a) & set(lignes_b)

    result = []
    for code in all_codes:
        la = lignes_a.get(code)
        lb = lignes_b.get(code)
        nature = (la or lb).get('nature','')
        if nature_filter and nature_filter not in nature.upper():
            continue
        result.append({
            'code':    code,
            'libelle': (la or lb).get('libelle',''),
            'nature':  nature,
            'commun':  code in communs,
            'poids_a': la.get('poids',0) if la else 0,
            'poids_b': lb.get('poids',0) if lb else 0,
            'valo_a':  la.get('valorisation',0) if la else 0,
            'valo_b':  lb.get('valorisation',0) if lb else 0,
        })
    result.sort(key=lambda x: max(x['poids_a'], x['poids_b']), reverse=True)
    return jsonify({
        'lignes':    result,
        'nb_commun': len(communs),
        'nb_only_a': len(set(lignes_a) - set(lignes_b)),
        'nb_only_b': len(set(lignes_b) - set(lignes_a)),
    })


# ═══════════════════════════════════════════════════════════════════════════
# SIMULATEUR RACHATS — impact en cascade sur fonds dans fonds
# ═══════════════════════════════════════════════════════════════════════════
@app.route('/simulateur')
def simulateur():
    fonds_list = _load_all_excel_fonds()
    return render_template('simulateur.html', fonds_list=fonds_list)

@app.route('/api/simulateur')
def api_simulateur():
    """
    Simule l'impact d'un rachat sur un fonds.
    Pour chaque ligne de portefeuille, calcule le montant vendu proportionnellement.
    Si une ligne est elle-même un OPCVM (fonds dans fonds), applique récursivement.
    """
    import urllib.parse
    fid        = request.args.get('fonds','')
    montant    = float(request.args.get('montant', 0) or 0)
    if not fid or montant <= 0:
        return jsonify({'error': 'Paramètres manquants'}), 400

    idx = _get_holdings_cache()
    # Construire un index nom_fonds → liste lignes
    fonds_lignes = {}
    for libelle, entries in idx.items():
        for e in entries:
            fn = e['fonds']
            if fn not in fonds_lignes:
                fonds_lignes[fn] = []
            fonds_lignes[fn].append({**e, 'libelle': libelle})

    # Nom du fonds cible
    fname     = urllib.parse.unquote(fid) + '.xlsx'
    fpath     = os.path.join(EXCEL_FOLDER, fname)
    fonds_obj = _read_excel_fonds(fpath) if os.path.exists(fpath) else {}
    fonds_nom = fonds_obj.get('fonds', fid)
    valo_tot  = fonds_obj.get('valo_globale_opcvm', 0) or 1

    # Simulation récursive (max 3 niveaux)
    def simulate(nom, montant_rachat, niveau=0, visited=None):
        if visited is None: visited = set()
        if nom in visited or niveau > 3: return []
        visited.add(nom)

        lignes = fonds_obj.get('lignes', []) if niveau == 0 else fonds_lignes.get(nom, [])
        valo   = fonds_obj.get('valo_globale_opcvm', 0) if niveau == 0 else \
                 sum(l.get('montant',0) for l in lignes) or 1

        impacts = []
        for l in lignes:
            poids     = l.get('poids', 0) / 100
            libelle   = l.get('libelle', l.get('valeur', ''))
            nature    = l.get('nature','')
            impact    = montant_rachat * poids

            entry = {
                'niveau':   niveau,
                'libelle':  libelle,
                'nature':   nature,
                'poids':    l.get('poids',0),
                'impact':   round(impact, 2),
                'fonds_parent': nom,
            }
            impacts.append(entry)

            # Si c'est un OPCVM, propager récursivement
            if 'FCP' in libelle.upper() or 'SICAV' in libelle.upper() or libelle.upper() in fonds_lignes:
                impacts += simulate(libelle.upper(), impact, niveau+1, visited.copy())

        return impacts

    impacts = simulate(fonds_nom, montant)
    impacts.sort(key=lambda x: (x['niveau'], -x['impact']))

    return jsonify({
        'fonds':   fonds_nom,
        'montant': montant,
        'impacts': impacts,
        'nb_titres_touches': len([i for i in impacts if i['niveau'] == 0]),
    })


if __name__ == '__main__':
    app.run(debug=True, port=5050)

# ═══════════════════════════════════════════════════════════════════════════
# OCR PARSER — for scanned PDFs (all pages are images, 0 words extracted)
# ═══════════════════════════════════════════════════════════════════════════
def parse_scanned_pdf(pdf_bytes, source_name='', soc_gestion=''):
    """
    Full OCR pipeline for scanned OPCVM PDFs (Medias24 format).
    1. Rotate each page 180° (Medias24 PDFs are upside-down)
    2. OCR with Tesseract (French)
    3. Parse inventory lines: ISIN, Désignation, Quantité, Valo.Globale, Poids
    """
    result = {
        'opcvm_name': source_name, 'gestionnaire': soc_gestion,
        'date': '', 'actif_net': 0, 'lignes': [], 'raw_page_count': 0,
        'parsed_at': datetime.datetime.now().isoformat(), 'source': source_name
    }

    try:
        from pdf2image import convert_from_bytes
        import pytesseract
        from PIL import ImageOps

        # Windows: set Tesseract path explicitly
        import platform
        if platform.system() == 'Windows':
            pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

        imgs = convert_from_bytes(pdf_bytes, dpi=250)
        result['raw_page_count'] = len(imgs)

        # OCR all pages, collect text
        all_text = ''
        for img in imgs:
            img_rot = img.rotate(180)
            text = pytesseract.image_to_string(img_rot, lang='fra', config='--psm 6 --oem 3')
            all_text += text + '\n'

        result['lignes'] = _parse_ocr_text(all_text)

        # Extract metadata
        for pat, key in [
            (r'OPCVM\s*:\s*(?:FCP|SICAV)?\s*([^\n\r]{3,60}?)(?:\s{3,}|\s*Date|\s*$)', 'opcvm_name'),
            (r'FCP\s+[«"](.*?)[»"]', 'opcvm_name'),
            (r'GESTIONNAIRE\s*:\s*([^\n\r]{3,50}?)(?:\s{3,}|\s*D[eé]p|\s*$)', 'gestionnaire'),
            (r'(?:Date inventaire|Data Inventaire|AU|CLOS LE)\s*[:\s]*(\d{2}[/.-]\d{2}[/.-]\d{4})', 'date'),
            (r'(\d{2}[/.-]\d{2}[/.-]\d{4})', 'date'),
        ]:
            if result[key]: continue
            m = re.search(pat, all_text, re.IGNORECASE)
            if m: result[key] = m.group(1).strip().strip('«»""').strip()

        # Total actif
        m = re.search(r'Total actif\s+([\d\s,\.]+)', all_text, re.IGNORECASE)
        if m:
            v, _ = clean_num(re.sub(r'\s','', m.group(1))[:20])
            result['actif_net'] = v

    except ImportError:
        result['error'] = 'OCR dependencies not installed (pdf2image, pytesseract)'
    except Exception as e:
        result['error'] = str(e)

    return result


def _parse_ocr_text(full_text):
    """
    Parse OCR text from Medias24 OPCVM inventory.
    Handles two patterns:
    A) Full line (page 15 style): EMETTEUR ISIN DESIGNATION QTE VALO_UNIT VALO_GLOBALE POIDS%
    B) Line without poids (page 16 style): EMETTEUR ISIN DESIGNATION QTE VALO_UNIT VALO_GLOBALE
       + separate column of poids% earlier in text
    """
    def fix_isin(s):
        return re.sub(r'[OQGD]', '0', s.upper())

    def parse_fr(s):
        s = str(s).strip().replace(' ','').replace('\xa0','').replace('/','')
        is_p = '%' in s; s = s.replace('%','')
        if s.count(',') == 1: s = s.replace(',','.')
        elif s.count(',') > 1:
            parts = s.split(','); s = ''.join(parts[:-1])+'.'+parts[-1]
        try: return float(s), is_p
        except: return 0.0, is_p

    ISIN_IN_LINE = re.compile(r'\b(MA[O0Oo]{0,2}\d{6,10})\b', re.IGNORECASE)

    lignes = []
    seen = set()

    # ── Pattern A: full structured line with poids at end ────────────────
    LINE_FULL = re.compile(
        r'(MA[O0Oo]{0,2}\d{6,10})\s+'    # ISIN
        r'(.{3,60}?)\s+'                   # DESIGNATION
        r'([\d][\d\s]*[,\.]?\d*)\s+'      # QTE
        r'([\d][\d\s]*[,\.]\d+)\s+'       # VALO UNIT
        r'([\d][\d\s]*[,\.]\d*)\s+'       # VALO GLOBALE
        r'([\d,\.]+%)',                    # POIDS
        re.MULTILINE
    )
    for m in LINE_FULL.finditer(full_text):
        isin = fix_isin(m.group(1))
        if isin in seen: continue
        desig = re.sub(r'^[\s_\.\-\|]+', '', m.group(2).strip()).upper()
        desig = re.sub(r'\s+', ' ', desig).strip()
        qte, _ = parse_fr(m.group(3))
        vglob, _ = parse_fr(m.group(5))
        poids, _ = parse_fr(m.group(6))
        if not desig: continue
        seen.add(isin)
        lignes.append({'isin': isin, 'valeur': desig, 'qte': qte,
                       'montant': vglob, 'poids': poids})

    # ── Pattern B: line without poids → match with standalone poids column ─
    # Find standalone poids values (lines containing only XX,XX% or XX.XX%)
    standalone_poids = []
    for line in full_text.split('\n'):
        line = line.strip()
        m_p = re.match(r'^\s*\|?\s*([\d]+[,\.]\d+)\s*%\s*$', line)
        if m_p:
            v, _ = parse_fr(m_p.group(1))
            if 0 < v <= 100:
                standalone_poids.append(v)

    # Lines with ISIN but NO poids at end
    no_poids_lines = []
    for line in full_text.split('\n'):
        line = line.strip()
        m_isin = ISIN_IN_LINE.search(line)
        if not m_isin: continue
        isin = fix_isin(m_isin.group(1))
        if isin in seen: continue
        # Check: does this line NOT have a % at the end?
        if re.search(r'[\d,\.]+%\s*$', line): continue

        # Extract designation and numbers
        after = line[m_isin.end():].strip()
        desig_m = re.match(r'^[|\s]*([A-ZÀ-Öa-zà-ö][A-ZÀ-Öa-zà-ö0-9\s\-\.&\(\)\'_]{2,50}?)(?=\s{2,}|\s+[\d]|$)', after)
        desig = desig_m.group(1).strip().upper() if desig_m else ''
        desig = re.sub(r'\s+', ' ', desig).strip()

        nums = re.findall(r'[\d][\d\s]*[,\.]\d+', after)
        vals = sorted([v for n in nums for v, _ in [parse_fr(n)] if v > 0], reverse=True)
        montant = vals[0] if vals else 0

        no_poids_lines.append({'isin': isin, 'valeur': desig or isin, 'montant': montant, 'poids': 0})

    # Align standalone_poids with no_poids_lines by position
    # The poids column appears BEFORE the data lines in page layout (left column)
    # We take the last N poids values where N = number of no-poids lines
    n = len(no_poids_lines)
    if n > 0 and standalone_poids:
        aligned = standalone_poids[-n:] if len(standalone_poids) >= n else \
                  standalone_poids + [0] * (n - len(standalone_poids))
        for i, ligne in enumerate(no_poids_lines):
            ligne['poids'] = aligned[i] if i < len(aligned) else 0

    for l in no_poids_lines:
        if l['isin'] not in seen:
            seen.add(l['isin'])
            lignes.append(l)

    return lignes