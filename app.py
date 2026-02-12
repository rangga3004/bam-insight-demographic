"""
BAM Insight — Top Omzet By Demographic
Streamlit Local App
"""

import os
import zipfile
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime

# --- Page Config ---
import streamlit as st

st.set_page_config(
    page_title="BAM Insight · Omzet Provinsi",
    page_icon="📊",
    layout="centered",
)

# --- Custom CSS to tighten layout ---
st.markdown("""
<style>
    .block-container { max-width: 900px; padding-top: 1.5rem; }
    [data-testid="stMetric"] { text-align: center; }
    [data-testid="stMetricLabel"] { justify-content: center; }
</style>
""", unsafe_allow_html=True)

# --- Constants ---
COL_STATUS   = 'Status Pesanan'
COL_HARGA    = 'Total Harga Produk'
COL_PROVINSI = 'Provinsi'
COL_TANGGAL  = 'Waktu Pesanan Dibuat'
COLS_NEEDED  = [COL_STATUS, COL_HARGA, COL_PROVINSI, COL_TANGGAL]

BULAN_INDO = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'Mei', 6: 'Jun',
    7: 'Jul', 8: 'Agu', 9: 'Sep', 10: 'Okt', 11: 'Nov', 12: 'Des'
}

HEADER_BG = '#1D388B'
HEADER_FG = '#FFFFFF'
PIE_COLORS = [
    '#325FEC', '#F4C144', '#6D50B8', '#96ADF5', '#1D388B',
    '#759EEE', '#E8B82E', '#0F0E7F', '#8A6FD0', '#5A8BF5',
    '#4A72E8', '#A3BEFF', '#D4A030', '#2E4BA0', '#B08EE8',
    '#7C9FF2',
]

# --- Matplotlib Settings ---
plt.rcParams['font.weight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['axes.titleweight'] = 'bold'


def format_rupiah(angka):
    return f"{angka:,.0f}".replace(",", ".")


def format_rupiah_singkat(angka):
    """Compact format: 2.030.018.664 → 2,03 M"""
    if angka >= 1_000_000_000:
        return f"Rp {angka / 1_000_000_000:,.2f} M"
    elif angka >= 1_000_000:
        return f"Rp {angka / 1_000_000:,.1f} Jt"
    else:
        return f"Rp {format_rupiah(angka)}"


def count_total_files(uploaded_files, mode):
    """Count total files to process (for progress bar)."""
    total = 0
    if mode == 'zip':
        for uf in uploaded_files:
            raw = uf.read()
            uf.seek(0)  # Reset for later read
            with zipfile.ZipFile(BytesIO(raw), 'r') as z:
                excel_files = [
                    f for f in z.namelist()
                    if (f.lower().endswith('.xlsx') or f.lower().endswith('.xls') or f.lower().endswith('.csv'))
                    and not f.startswith('__MACOSX')
                    and not os.path.basename(f).startswith('~$')
                ]
                total += len(excel_files)
    else:
        total = len(uploaded_files)
    return total


def read_files(uploaded_files, mode, progress_bar, status_text):
    """Read uploaded files into a list of DataFrames with progress."""
    all_dfs = []
    total = count_total_files(uploaded_files, mode)
    done = 0

    for uf in uploaded_files:
        if mode == 'zip':
            with zipfile.ZipFile(BytesIO(uf.read()), 'r') as z:
                excel_files = [
                    f for f in z.namelist()
                    if (f.lower().endswith('.xlsx') or f.lower().endswith('.xls') or f.lower().endswith('.csv'))
                    and not f.startswith('__MACOSX')
                    and not os.path.basename(f).startswith('~$')
                ]
                for ef in sorted(excel_files):
                    base = os.path.basename(ef)
                    status_text.text(f"📖 Membaca: {base}  ({done + 1}/{total})")
                    try:
                        with z.open(ef) as f:
                            file_bytes = BytesIO(f.read())
                        if ef.lower().endswith('.csv'):
                            df_part = pd.read_csv(file_bytes)
                        else:
                            df_part = pd.read_excel(file_bytes, engine='openpyxl', dtype=str)
                        all_dfs.append(df_part)
                    except Exception:
                        st.warning(f"Gagal membaca: {base}")
                    done += 1
                    progress_bar.progress(done / total, text=f"{int(done / total * 100)}%")

        elif mode == 'excel':
            status_text.text(f"📖 Membaca: {uf.name}  ({done + 1}/{total})")
            df_part = pd.read_excel(BytesIO(uf.read()), engine='openpyxl', dtype=str)
            all_dfs.append(df_part)
            done += 1
            progress_bar.progress(done / total, text=f"{int(done / total * 100)}%")

        elif mode == 'csv':
            status_text.text(f"📖 Membaca: {uf.name}  ({done + 1}/{total})")
            df_part = pd.read_csv(uf)
            all_dfs.append(df_part)
            done += 1
            progress_bar.progress(done / total, text=f"{int(done / total * 100)}%")

    status_text.text("✅ Semua file berhasil dibaca!")

    if not all_dfs:
        return None
    return pd.concat(all_dfs, ignore_index=True, sort=False)


def proses_data(df, exclude_batal=True):
    """Process data: filter, fix numbers, calculate."""

    # Detect period
    periode = "Tidak Diketahui"
    if COL_TANGGAL in df.columns:
        df[COL_TANGGAL] = pd.to_datetime(df[COL_TANGGAL], errors='coerce')
        tgl_min = df[COL_TANGGAL].min()
        tgl_max = df[COL_TANGGAL].max()
        if pd.notna(tgl_min) and pd.notna(tgl_max):
            if tgl_min.month == tgl_max.month and tgl_min.year == tgl_max.year:
                periode = f"{BULAN_INDO[tgl_min.month]} {tgl_min.year}"
            else:
                periode = f"{BULAN_INDO[tgl_min.month]} {tgl_min.year} - {BULAN_INDO[tgl_max.month]} {tgl_max.year}"

    # Filter cancelled orders
    if exclude_batal:
        df_valid = df[~df[COL_STATUS].str.lower().str.contains('batal', na=False)].copy()
    else:
        df_valid = df.copy()

    # Fix Indonesian number format: dot=thousands, comma=decimal
    df_valid[COL_HARGA] = (
        df_valid[COL_HARGA]
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
    )
    df_valid[COL_HARGA] = pd.to_numeric(df_valid[COL_HARGA], errors='coerce').fillna(0)

    total_omzet = df_valid[COL_HARGA].sum()

    omzet_prov = (
        df_valid
        .groupby(COL_PROVINSI)[COL_HARGA]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={COL_HARGA: 'Total Omzet'})
    )
    omzet_prov['Persen'] = (omzet_prov['Total Omzet'] / total_omzet * 100).round(1)

    return periode, total_omzet, df_valid, omzet_prov


def buat_tabel(periode, total_omzet, omzet_prov):
    """Generate Top 10 table as matplotlib figure."""
    top_10 = omzet_prov.head(10)

    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='white')
    ax.axis('off')
    ax.set_facecolor('white')

    ax.set_title(
        f'TOP 10 PROVINSI BY OMZET\nPeriode: {periode}  ·  Total Omzet: Rp {format_rupiah(total_omzet)}',
        fontsize=11, fontweight='bold', color='black', pad=14, linespacing=1.5
    )

    col_labels = ['No', 'Provinsi', 'Total Omzet (Rp)', 'Persentase']
    table_data = []
    for i, (_, row) in enumerate(top_10.iterrows(), 1):
        table_data.append([str(i), row[COL_PROVINSI], format_rupiah(row['Total Omzet']), f"{row['Persen']}%"])

    table = ax.table(cellText=table_data, colLabels=col_labels, loc='center',
                     cellLoc='center', colWidths=[0.08, 0.35, 0.32, 0.15])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)

    for j in range(len(col_labels)):
        cell = table[0, j]
        cell.set_facecolor(HEADER_BG)
        cell.set_text_props(color=HEADER_FG, fontweight='bold', fontsize=10)
        cell.set_edgecolor('#CCCCCC')

    for i in range(1, len(table_data) + 1):
        for j in range(len(col_labels)):
            cell = table[i, j]
            cell.set_facecolor('#FFFFFF' if i % 2 == 1 else '#F5F5F5')
            cell.set_text_props(color='black', fontsize=9, fontweight='medium')
            cell.set_edgecolor('#CCCCCC')
        table[i, 1].set_text_props(ha='left')
        table[i, 1]._loc = 'left'

    fig.text(0.5, 0.02, 'Data source: Shopee Order Export  ·  Status ≠ Batal',
             fontsize=8, color='#666666', ha='center', style='italic')

    plt.tight_layout()
    return fig


def buat_pie_chart(periode, total_omzet, omzet_prov):
    """Generate Pie chart as matplotlib figure."""
    pie_data = omzet_prov.copy()
    threshold = 1.5
    small_mask = pie_data['Persen'] < threshold
    if small_mask.any():
        lainnya_total = pie_data.loc[small_mask, 'Total Omzet'].sum()
        lainnya_persen = round(pie_data.loc[small_mask, 'Persen'].sum(), 1)
        pie_main = pie_data[~small_mask].copy()
        pie_main = pd.concat([pie_main, pd.DataFrame([{
            COL_PROVINSI: 'Lainnya', 'Total Omzet': lainnya_total, 'Persen': lainnya_persen
        }])], ignore_index=True)
    else:
        pie_main = pie_data.copy()

    fig, ax = plt.subplots(figsize=(8, 6), facecolor='white')
    ax.set_facecolor('white')

    colors = PIE_COLORS[:len(pie_main)]

    wedges, texts, autotexts = ax.pie(
        pie_main['Total Omzet'], labels=None,
        autopct=lambda pct: f'{pct:.1f}%' if pct >= 3 else '',
        colors=colors, startangle=90, counterclock=False, pctdistance=0.78,
        textprops={'fontsize': 10, 'color': 'white', 'fontweight': 'bold'},
        wedgeprops={'edgecolor': 'white', 'linewidth': 1.5}
    )

    fig.suptitle(
        f'Distribusi Omzet per Provinsi\nPeriode: {periode}  ·  Total Omzet: Rp {format_rupiah(total_omzet)}',
        fontsize=12, fontweight='bold', color='black', y=0.96, linespacing=1.5
    )

    ax.legend(
        wedges,
        [f'{row[COL_PROVINSI]}  ({row["Persen"]}%)' for _, row in pie_main.iterrows()],
        loc='center left', bbox_to_anchor=(1.05, 0.5),
        fontsize=8.5, frameon=True, fancybox=True,
        facecolor='white', edgecolor='#CCCCCC',
        labelcolor='black', handlelength=1.0, handleheight=0.9, labelspacing=0.6
    )

    fig.text(0.5, 0.03, 'Data source: Shopee Order Export  ·  Status ≠ Batal',
             fontsize=7, color='#888888', ha='center', style='italic')

    fig.subplots_adjust(left=0.05, right=0.58, top=0.86, bottom=0.08)

    return fig


def fig_to_bytes(fig):
    """Convert matplotlib figure to PNG bytes."""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, facecolor='white', bbox_inches='tight', pad_inches=0.4)
    buf.seek(0)
    return buf.getvalue()


# =============================================================
# STREAMLIT UI
# =============================================================

st.title("📊 BAM Insight")
st.subheader("Top Omzet By Demographic — Analisis per Provinsi")
st.markdown("---")

# --- Upload Section ---
st.sidebar.header("📤 Upload Data")
mode = st.sidebar.radio(
    "Pilih format file:",
    options=['zip', 'excel', 'csv'],
    format_func=lambda x: {
        'zip': '📦 ZIP (berisi multipart Excel)',
        'excel': '📄 Excel satuan (.xlsx)',
        'csv': '⚡ CSV (paling cepat)',
    }[x],
    index=0
)

file_types = {
    'zip': ['zip'],
    'excel': ['xlsx', 'xls'],
    'csv': ['csv'],
}

uploaded_files = st.sidebar.file_uploader(
    f"Upload file {mode.upper()}",
    type=file_types[mode],
    accept_multiple_files=True,
    help="Drag & drop atau klik untuk upload. Bisa lebih dari satu file."
)

st.sidebar.markdown("---")
st.sidebar.header("⚙️ Filter")
exclude_batal = st.sidebar.toggle(
    "Exclude Pesanan Batal",
    value=True,
    help="Default: ON (exclude). Matikan untuk include semua pesanan termasuk yang batal."
)

if uploaded_files:
    st.markdown("### 📂 Membaca Data...")
    progress_bar = st.progress(0, text="0%")
    status_text = st.empty()
    df = read_files(uploaded_files, mode, progress_bar, status_text)

    if df is None or len(df) == 0:
        st.error("Tidak ada data yang berhasil dibaca.")
        st.stop()

    st.success(f"✅ Data berhasil dimuat: **{len(df):,}** baris, **{len(df.columns)}** kolom")

    # --- Process Data ---
    with st.spinner("Menganalisis data..."):
        periode, total_omzet, df_valid, omzet_prov = proses_data(df, exclude_batal)

    # --- Summary Metrics ---
    st.markdown("### 📋 Ringkasan")
    col1, col2, col3, col4 = st.columns([1, 1.5, 1, 1])
    col1.metric("Periode", periode)
    col2.metric("Total Omzet", format_rupiah_singkat(total_omzet))
    col3.metric("Pesanan Valid", f"{len(df_valid):,}")
    col4.metric("Jumlah Provinsi", df_valid[COL_PROVINSI].nunique())

    st.markdown("---")

    # --- Table & Chart ---
    top_10 = omzet_prov.head(10)
    tab1, tab2, tab3 = st.tabs(["📊 Tabel Top 10", "🥧 Pie Chart", "📥 Download"])

    with tab1:
        fig_tabel = buat_tabel(periode, total_omzet, omzet_prov)
        st.pyplot(fig_tabel)
        plt.close(fig_tabel)

        with st.expander("🔍 Lihat Detail Data Mentah (100 Baris Pertama)"):
            st.dataframe(df_valid.head(100), use_container_width=True)

    with tab2:
        fig_pie = buat_pie_chart(periode, total_omzet, omzet_prov)
        st.pyplot(fig_pie)
        plt.close(fig_pie)

    with tab3:
        st.markdown("### 📥 Download Hasil")

        # Generate PNGs only when needed for download
        fig_t = buat_tabel(periode, total_omzet, omzet_prov)
        fig_p = buat_pie_chart(periode, total_omzet, omzet_prov)
        tabel_bytes = fig_to_bytes(fig_t)
        pie_bytes = fig_to_bytes(fig_p)
        plt.close(fig_t)
        plt.close(fig_p)

        # CSV merged
        csv_buf = BytesIO()
        df.to_csv(csv_buf, index=False, encoding='utf-8-sig')
        csv_bytes = csv_buf.getvalue()

        periode_safe = periode.replace(' ', '_').replace('-', 'to')

        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "📊 Download Tabel PNG",
                data=tabel_bytes,
                file_name=f"Tabel_Top10_{periode_safe}.png",
                mime="image/png"
            )
        with c2:
            st.download_button(
                "🥧 Download Pie Chart PNG",
                data=pie_bytes,
                file_name=f"PieChart_{periode_safe}.png",
                mime="image/png"
            )
        with c3:
            st.download_button(
                "⚡ Download Merged CSV",
                data=csv_bytes,
                file_name=f"merged_{periode_safe}.csv",
                mime="text/csv"
            )
        st.info("💡 **Tip:** Download CSV merged, lalu upload lagi pakai mode CSV untuk proses ulang yang lebih cepat!")

else:
    st.info("👈 Upload file di sidebar kiri untuk mulai analisis.")
    st.markdown("""
    ### Cara Pakai
    1. **Pilih format** di sidebar kiri (ZIP / Excel / CSV)
    2. **Upload file** — bisa lebih dari satu
    3. **Lihat hasil** — tabel & pie chart otomatis muncul
    4. **Download** — PNG chart + CSV merged

    ### Format yang Didukung
    | Mode | File | Kecepatan |
    |------|------|-----------|
    | 📦 ZIP | ZIP berisi multipart Excel | Normal |
    | 📄 Excel | File `.xlsx` satuan | Normal |
    | ⚡ CSV | File `.csv` | **Tercepat** |

    > **💡 Tip:** Setelah pertama kali proses, download CSV merged.
    > Upload CSV itu di lain waktu untuk proses yang jauh lebih cepat!
    """)
