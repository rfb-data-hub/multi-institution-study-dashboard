import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import google.cloud.bigquery as bq

    client = bq.Client(project="flow-battery-data-ingestion")

    df_metrics = pl.from_arrow(
        client.query("""
            SELECT Phase, ParticipantID, FlowRate, Repeat,
                   RowType, CycleNumber, IsOutlier,
                   ChargeCapacity_mAh, DischargeCapacity_mAh,
                   CoulombicEfficiency_pct, VoltageEfficiency_pct,
                   EnergyEfficiency_pct, ElectrolyteUtilisation_pct,
                   CapacityDecay_pct_per_cycle
            FROM `flow-battery-data-ingestion.electrochem.charge-discharge-metrics`
            WHERE RowType = 'cycle'
        """).to_arrow()
    )

    df_meta_2a = pl.DataFrame({
        "ParticipantID": ["P02","P03","P05","P06","P07","P08","P09","P11","P12","P13","P14","P15","P16","P18","P19","P20","P21","P22","P24","P25","P26","P28","P29","P30","P31","P32","P33","P34","P35","P36","P37","P39","P40","P41","P43","P44","P45","P46"],
        "Cell": ["Custom","MIT Gen 2","Custom","Custom","GGM","FCT","Custom","Custom","FCT","MIT Gen 2","MIT Gen 2","FCT","QUB","Custom","FCT","Custom","MIT Gen 2","MIT Gen 2","Custom","PinFlow","QUB","MIT Gen 2","FCT","Custom","QUB","MIT Gen 2","Scribner","QUB","Custom","FCT","FCT","QUB","Custom","Custom","FCT","Custom","Custom","QUB"],
        "FitCarbonPaper": ["No","Yes","No","No","Yes","Yes","No","No","Yes","Yes","Yes","Yes","No","No","Yes","Possible","Yes","Yes","No","No","No","Yes","Yes","No","No","Yes","Yes","No","Yes","Yes","Yes","No","No","No","Yes","No","No","No"],
        "FF": ["FTFF","IDFF","FTFF","FTFF","IDFF","SFF","FTFF","FTFF","SFF","IDFF","FTFF","IDFF","FTFF","FTFF","SFF","FTFF","IDFF","IDFF","FTFF","FTFF","FTFF","IDFF","SFF","FTFF","FTFF","FTFF","SFF","FTFF","IDFF","IDFF","SFF","FTFF","FTFF","FTFF","SFF","FTFF","FTFF","FTFF"],
        "Area_cm2": [5.3,2.5,5.3,10,5.0625,5,19,5.3,5,2.5,2.5,30,16,5.3,5,25,2.6,2.5,11,50,16,2.24,5,5,25,2.5,5,16,4,5,5,16,20,10,5,3.1,5.3,16],
        "Membrane": ["FS-930","N212","FS-930","N212","N117","N117","FS-930","FS-930","N117","N212","N117","N117","Aquivion","FS-930","N212","FS-930","FS E-620-K","N117","N212","N212","Aquivion","N117","FS E-630-K","FS E-620-K","N117","N212","N212","Aquivion","FS-930","FS E-630-K","FS E-630-K","Aquivion","Aquivion","N117","N117","N117","FS-930","Aquivion"],
        "Electrode": ["SGL GFA 6","FBERG H23","SGL GFA 6","SGL GFD 4.65 EA","SGL 39 AA","SGL 39 AA","SGL GFD 4.65 EA","SGL GFA 6","SGL GFD 2.5 EA","FBERG E20","SGL GFD 4.65 EA","AvCarb G280A","SGL GFD 4.65 EA","SGL GFA 6","SGL 39 AA","SGL GFD 4.65 EA","SGL 29 AA","SGL 39 AA","SGL GFD 2.5 EA","SGL GFD 4.65","SGL GFD 4.65 EA","FBERG H23","SGL 39 AA","SGL GFD 2.5 EA","Mersen carbon felt","FBERG E20","SGL GFD 4.65 EA","SGL GFD 4.65 EA","ELAT","SGL 39 AA","FBERG H23","SGL GFD 4.65 EA","SGL GFA 6 EA","AvCarb G280A","SGL GFD 2.5 EA","Carbon felt, 6.35mm","SGL GFA 6","SGL GFD 4.65 EA"],
    })

    df_meta_2b = pl.from_arrow(
        client.query("""
            SELECT *
            FROM `flow-battery-data-ingestion.electrochem.participant-metadata`
        """).to_arrow()
    )

    def _custom_theme():
        return {
            "config": {
                "font": "Open Sans",
                "axis": {
                    "labelFontSize": 13,
                    "titleFontSize": 14,
                    "labelFont": "Open Sans",
                    "titleFont": "Open Sans",
                },
                "title": {
                    "fontSize": 16,
                    "fontWeight": "bold",
                    "font": "Open Sans",
                },
                "legend": {
                    "labelFontSize": 12,
                    "titleFontSize": 13,
                    "labelFont": "Open Sans",
                    "titleFont": "Open Sans",
                }
            }
        }

    alt.themes.register("custom", _custom_theme)
    alt.themes.enable("custom")
    alt.renderers.set_embed_options(renderer="svg")
    None
    return alt, client, df_meta_2a, df_meta_2b, df_metrics, mo, pl


@app.cell
def _(mo):
    mo.Html("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;700&display=swap');

    * {
        font-family: 'Open Sans', sans-serif !important;
    }
    </style>
    """)
    return


@app.cell
def _(mo):
    mo.vstack([
        mo.center(
            mo.hstack([
                mo.image("Flow_battery_comms_logo.png", width=120),
                mo.vstack([
                    mo.Html("<h1 style='font-size:42px; font-weight:800; margin:0;'>Flow Battery Reproducibility Study</h1>"),
                    mo.md("### International Multi-Institution Data Dashboard"),
                ]),
            ], align="center", gap=2)
        ),
        mo.callout(mo.md("""
            This dashboard displays data and metadata collected during our **second** international multi-institution study.
            The intention of this data and broader studies is to determine how our experimental practices influence
            the reliability of our data and to eventually arrive at community developed standards.
            To our knowledge, this is the **largest database of flow battery repeat testing from multiple institutions!**
            If you are viewing this as a participant, thank you so much for your efforts — we hope you have enjoyed
            being a part of this study and it has benefited your research. If you have stumbled upon this dashboard
            and want to get involved, please contact:
            [Josh](mailto:j.bailey@qub.ac.uk) · [Hugh](mailto:h.oconnor@qub.ac.uk) · [Fik](mailto:brushett@mit.edu) · [Alex](mailto:quinnale@mit.edu)
        """), kind="info"),
        mo.center(mo.image("institution_map.png", width=800)),
        mo.md("The study was conducted in two phases, both using **0.2 M ferri/ferrocyanide in 1 M KCl** with the same techniques: charge-discharge, polarisation, and EIS."),
    ], gap=1)
    return


@app.cell
def _(mo):
    mo.hstack([
        mo.Html('<div style="flex:1; padding:16px; border:1px solid #ddd; border-radius:8px;"><b>Phase 2a</b> — Participants used their own cells and calculated superficial velocities and tank volumes based on cell architecture/active area.</div>'),
        mo.Html('<div style="flex:1; padding:16px; border:1px solid #ddd; border-radius:8px;"><b>Phase 2b</b> — Participants used an identical 3D-printed cell distributed from QUB and ran their experiments using 100 mL of electrolyte at 25 mL min<sup>−1</sup>.</div>'),
    ], gap=2, justify="space-between")
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">📊 Overall Summary Statistics</h3>
    </div>
    """)
    return


@app.cell
def _(client, mo, pl):
    _df_meta = pl.from_arrow(
        client.query("""
            SELECT
                COUNT(DISTINCT SourceFileID) as total_datasets,
                COUNT(DISTINCT CASE WHEN Phase = '2a' THEN SourceFileID END) as datasets_2a,
                COUNT(DISTINCT CASE WHEN Phase = '2b' THEN SourceFileID END) as datasets_2b,
                COUNT(*) as total_rows,
                MAX(IngestedAt) as latest_ingest
            FROM `flow-battery-data-ingestion.electrochem.charge-discharge-data`
        """).to_arrow()
    ).row(0, named=True)
    _latest = str(_df_meta["latest_ingest"])[:16].replace("T", " ")
    mo.Html(f"""
    <div style="display:flex; gap:16px; flex-wrap:wrap; margin: 8px 0 24px 0; justify-content:center;">
      <div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:10px; padding:14px 24px; text-align:center; flex:1; min-width:140px; max-width:200px;">
        <div style="font-size:28px; font-weight:800; color:#16a34a;">{_df_meta["total_datasets"]}</div>
        <div style="font-size:12px; color:#555; margin-top:4px;">Total Datasets</div>
      </div>
      <div style="background:#f8f8ff; border:1px solid #e0e0f0; border-radius:10px; padding:14px 24px; text-align:center; flex:1; min-width:140px; max-width:200px;">
        <div style="font-size:28px; font-weight:800; color:#6366F1;">{_df_meta["datasets_2a"]}</div>
        <div style="font-size:12px; color:#555; margin-top:4px;">Phase 2a Datasets</div>
      </div>
      <div style="background:#fff8ee; border:1px solid #f0e8d0; border-radius:10px; padding:14px 24px; text-align:center; flex:1; min-width:140px; max-width:200px;">
        <div style="font-size:28px; font-weight:800; color:#F59E0B;">{_df_meta["datasets_2b"]}</div>
        <div style="font-size:12px; color:#555; margin-top:4px;">Phase 2b Datasets</div>
      </div>
      <div style="background:#fdf2f8; border:1px solid #f0d0e8; border-radius:10px; padding:14px 24px; text-align:center; flex:1; min-width:140px; max-width:200px;">
        <div style="font-size:28px; font-weight:800; color:#db2777;">{_df_meta["total_rows"]:,}</div>
        <div style="font-size:12px; color:#555; margin-top:4px;">Total Data Rows</div>
      </div>
      <div style="background:#f0f9ff; border:1px solid #bae6fd; border-radius:10px; padding:14px 24px; text-align:center; flex:1; min-width:140px; max-width:200px;">
        <div style="font-size:18px; font-weight:700; color:#0284c7; margin-top:4px;">{_latest}</div>
        <div style="font-size:12px; color:#555; margin-top:4px;">Latest Data Ingested</div>
      </div>
    </div>
    """)
    return


@app.cell
def _(alt, df_metrics, mo, pl):
    _df_summary = df_metrics.filter(pl.col("IsOutlier") == False)

    _metrics_map = {
        "Coulombic Efficiency (%)": "CoulombicEfficiency_pct",
        "Voltage Efficiency (%)": "VoltageEfficiency_pct",
        "Energy Efficiency (%)": "EnergyEfficiency_pct",
        "Electrolyte Utilisation (%)": "ElectrolyteUtilisation_pct",
        "Capacity Decay (%/cycle)": "CapacityDecay_pct_per_cycle",
    }

    _phase_colors = {"2a": "#6366F1", "2b": "#F59E0B"}

    _stats_rows = []
    for _phase in ["2a", "2b"]:
        _df_phase = _df_summary.filter(pl.col("Phase") == _phase)
        for _label, _col in _metrics_map.items():
            if len(_df_phase) > 0:
                _mean = round(_df_phase[_col].mean(), 2)
                _stats_rows.append({
                    "Metric": _label,
                    "Phase": _phase,
                    "Mean Value (%)": _mean,
                    "Mean Capacity Decay (%/cycle)": _mean,
                    "Median": round(_df_phase[_col].median(), 2),
                    "StdDev": round(_df_phase[_col].std(), 2),
                })
    _df_stats = pl.DataFrame(_stats_rows) if _stats_rows else pl.DataFrame({
        "Metric": [], "Phase": [], "Mean Value (%)": [], "Mean Capacity Decay (%/cycle)": [], "Median": [], "StdDev": []
    })

    _pct_metrics = [m for m in _metrics_map.keys() if "%/cycle" not in m]
    _cap_metrics = [m for m in _metrics_map.keys() if "%/cycle" in m]
    _df_pct = _df_stats.filter(~pl.col("Metric").str.contains("%/cycle")).to_pandas()
    _df_cap = _df_stats.filter(pl.col("Metric").str.contains("%/cycle")).to_pandas()

    _col_header = alt.Header(
        labelAngle=-30,
        labelAlign="right",
        labelFontSize=13,
        labelFont="Open Sans",
        labelFontWeight="normal",
        labelPadding=15,
        labelOrient="bottom",
    )

    _base_pct = alt.Chart(_df_pct)
    _chart_pct = alt.layer(
        _base_pct.mark_bar(width=30).encode(
            x=alt.X("Phase:N", title=None, axis=None),
            y=alt.Y("Mean Value (%):Q", scale=alt.Scale()),
            color=alt.Color("Phase:N",
                scale=alt.Scale(domain=["2a", "2b"], range=["#6366F1", "#F59E0B"]),
                legend=None),
            tooltip=["Metric", "Phase", "Mean Value (%)", "Median", "StdDev"]
        ),
        _base_pct.mark_errorbar(color="black", ticks=True, thickness=1, size=5).encode(
            x=alt.X("Phase:N"),
            y=alt.Y("Mean Value (%):Q", scale=alt.Scale()),
            yError=alt.YError("StdDev:Q"),
        )
    ).properties(width=80, height=300).facet(
        column=alt.Column("Metric:N", title=None, sort=_pct_metrics, header=_col_header)
    )

    _base_cap = alt.Chart(_df_cap)
    _chart_cap = alt.layer(
        _base_cap.mark_bar(width=30).encode(
            x=alt.X("Phase:N", title=None, axis=None),
            y=alt.Y("Mean Capacity Decay (%/cycle):Q", scale=alt.Scale()),
            color=alt.Color("Phase:N",
                scale=alt.Scale(domain=["2a", "2b"], range=["#6366F1", "#F59E0B"]),
                legend=None),
            tooltip=["Metric", "Phase", "Mean Capacity Decay (%/cycle)", "Median", "StdDev"]
        ),
        _base_cap.mark_errorbar(color="black", ticks=True, thickness=1, size=5).encode(
            x=alt.X("Phase:N"),
            y=alt.Y("Mean Capacity Decay (%/cycle):Q", scale=alt.Scale()),
            yError=alt.YError("StdDev:Q"),
        )
    ).properties(width=80, height=300).facet(
        column=alt.Column("Metric:N", title=None, sort=_cap_metrics, header=_col_header)
    )

    _combined_chart = alt.hconcat(_chart_pct, _chart_cap).resolve_scale(y="independent")

    _legend_html = mo.Html("""
    <div style="display:flex; justify-content:center; align-items:center; gap:32px; margin-bottom:8px;">
      <div style="display:flex; align-items:center; gap:10px;">
        <div style="width:24px;height:24px;border-radius:4px;background:#6366F1;"></div>
        <span style="font-size:18px; font-weight:700; font-family:Open Sans;">Phase 2a</span>
      </div>
      <div style="display:flex; align-items:center; gap:10px;">
        <div style="width:24px;height:24px;border-radius:4px;background:#F59E0B;"></div>
        <span style="font-size:18px; font-weight:700; font-family:Open Sans;">Phase 2b</span>
      </div>
    </div>
    """)

    _table_rows_html = ""
    for _phase in ["2a", "2b"]:
        _color = _phase_colors[_phase]
        _swatch = f'<div style="width:14px;height:14px;border-radius:3px;background:{_color};display:inline-block;vertical-align:middle;margin-right:6px;"></div>'
        _phase_rows = [r for r in _df_stats.to_dicts() if r["Phase"] == _phase]
        for _i, _row in enumerate(_phase_rows):
            _phase_cell = (
                f'<td style="padding:6px 14px; font-weight:600; border-bottom:1px solid #f0f0f0; vertical-align:top;">{_swatch}Phase {_phase}</td>'
                if _i == 0 else
                f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0;"></td>'
            )
            _table_rows_html += (
                f'<tr>{_phase_cell}'
                f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0;">{_row["Metric"]}</td>'
                f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0;">{_row["Mean Value (%)"]:.2f}</td>'
                f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0;">{_row["Median"]:.2f}</td>'
                f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0;">{_row["StdDev"]:.2f}</td>'
                f'</tr>'
            )

    _table_html = mo.Html(f"""
    <div style="overflow-x:auto; display:flex; justify-content:center;">
    <table style="border-collapse:collapse; font-size:14px;">
      <thead>
        <tr style="border-bottom: 2px solid #e5e7eb;">
          <th style="padding:6px 14px; text-align:left;">Phase</th>
          <th style="padding:6px 14px; text-align:left;">Metric</th>
          <th style="padding:6px 14px; text-align:left;">Mean</th>
          <th style="padding:6px 14px; text-align:left;">Median</th>
          <th style="padding:6px 14px; text-align:left;">Std Dev</th>
        </tr>
      </thead>
      <tbody>{_table_rows_html}</tbody>
    </table>
    </div>
    """)

    alt.data_transformers.enable("default", max_rows=None)

    mo.vstack([
        _legend_html,
        mo.Html(f'<div style="overflow-x:auto; text-align:center;">{mo.as_html(_combined_chart).text}</div>'),
        _table_html,
    ], gap=2)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">🔬 Phase 2a Participant Metadata</h3>
    </div>
    """)
    return


@app.cell
def _(alt, df_meta_2a, df_pol, mo, pl):
    alt.renderers.set_embed_options(renderer="svg")
    _pie_cols_2a = {
        "Cell": "Cell",
        "Flow Field": "FF",
        "Membrane": "Membrane",
        "Electrode": "Electrode",
    }
    _charts_2a = []
    for _title, _col in _pie_cols_2a.items():
        _df_counts = (
            df_meta_2a
            .group_by(_col)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .to_pandas()
        )
        _chart = alt.Chart(_df_counts).mark_arc(stroke="white", strokeWidth=1).encode(
            theta=alt.Theta("count:Q"),
            color=alt.Color(f"{_col}:N", legend=alt.Legend(orient="bottom", columns=2, title=None)),
            tooltip=[alt.Tooltip(f"{_col}:N", title=_title), alt.Tooltip("count:Q", title="Count")]
        ).properties(
            title=alt.TitleParams(_title, anchor="middle"),
            width=160,
            height=160,
        )
        _charts_2a.append(_chart)
    _row1_2a = alt.hconcat(_charts_2a[0], _charts_2a[1], spacing=80).resolve_scale(color="independent")
    _row2_2a = alt.hconcat(_charts_2a[2], _charts_2a[3], spacing=80).resolve_scale(color="independent")
    _pie_chart_2a = alt.vconcat(_row1_2a, _row2_2a, spacing=40).resolve_scale(color="independent")
    _df_area = df_meta_2a.sort("Area_cm2").to_pandas()
    _df_flowrates = (
        df_pol
        .filter(pl.col("Phase") == "2a")
        .select(["ParticipantID", "FlowRate"])
        .unique()
        .sort(["ParticipantID", "FlowRate"])
    )
    _df_flowrates = _df_flowrates.with_columns(
        pl.col("FlowRate")
        .rank(method="ordinal")
        .over("ParticipantID")
        .alias("Rank")
    ).with_columns(
        pl.when(pl.col("Rank") == 1).then(pl.lit("Low"))
        .when(pl.col("Rank") == 2).then(pl.lit("Medium"))
        .otherwise(pl.lit("High"))
        .alias("FlowLevel")
    ).to_pandas()
    _participant_order = _df_area.sort_values("Area_cm2")["ParticipantID"].tolist()
    _base = alt.Chart().encode(
        x=alt.X("ParticipantID:N", sort=_participant_order, title=None)
    )
    _area_dots = _base.mark_circle(size=120, color="#6366F1").encode(
        y=alt.Y("Area_cm2:Q", title="Electrode Area (cm²)", axis=alt.Axis(titleColor="#6366F1")),
        tooltip=[
            alt.Tooltip("ParticipantID:N", title="Participant"),
            alt.Tooltip("Area_cm2:Q", title="Area (cm²)"),
            alt.Tooltip("Cell:N", title="Cell"),
            alt.Tooltip("Membrane:N", title="Membrane"),
        ]
    ).transform_lookup(
        lookup="ParticipantID",
        from_=alt.LookupData(data=_df_area, key="ParticipantID", fields=["Area_cm2", "Cell", "Membrane"])
    )
    _flow_dots = _base.mark_point(size=80, filled=False, color="#F59E0B", strokeWidth=2).encode(
        y=alt.Y("FlowRate:Q", title="Flow Rate (mL/min)", axis=alt.Axis(titleColor="#F59E0B")),
        shape=alt.Shape(
            "FlowLevel:N",
            scale=alt.Scale(
                domain=["Low", "Medium", "High"],
                range=["triangle-down", "square", "triangle-up"]
            ),
            legend=alt.Legend(title="Flow Rate Level")
        ),
        tooltip=[
            alt.Tooltip("ParticipantID:N", title="Participant"),
            alt.Tooltip("FlowRate:Q", title="Flow Rate (mL/min)"),
            alt.Tooltip("FlowLevel:N", title="Level"),
        ]
    )
    _dot_plot = alt.layer(
        _flow_dots.properties(data=_df_flowrates),
        _area_dots.properties(data=_df_area),
    ).resolve_scale(
        y="independent"
    ).properties(
        title="Electrode Area and Flow Rates by Participant",
        width="container",
        height=300,
    )
    mo.vstack([
        mo.Html(f'<div style="overflow-x:auto; text-align:center;">{mo.as_html(_pie_chart_2a).text}</div>'),
        mo.Html(f'<div style="overflow-x:hidden; width:100%;"><div style="max-width:1000px; margin:0 auto;">{mo.as_html(_dot_plot).text}</div></div>'),
    ], gap=2)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">🔬 Phase 2b Participant Metadata</h3>
    </div>
    """)
    return


@app.cell
def _(alt, df_meta_2b, mo, pl):
    alt.renderers.set_embed_options(renderer="svg")
    _df_meta_2b = df_meta_2b.with_columns(
        pl.when(pl.col("PotentiostatManufacturer") == "")
        .then(pl.lit("Multiple"))
        .otherwise(pl.col("PotentiostatManufacturer"))
        .alias("PotentiostatManufacturer")
    ).with_columns(
        pl.when(pl.col("ElectricalConnectionScheme") == "")
        .then(pl.lit("Unknown"))
        .otherwise(pl.col("ElectricalConnectionScheme"))
        .alias("ElectricalConnectionScheme")
    ).with_columns(
        pl.col("ElectrodeCuttingMethod")
        .replace({
            "Automated laser cutting for guide ad manual razor cutting for the remaining depth": "Laser & freehand",
            "Cookie cutter was used to cut the electrodes to size": "Cookie cutter",
            "Cutting mat for measurement and carpet knife for cutting guided by a ruler": "Carpet knife",
            "Freehand cutting using scissors": "Scissors (freehand)",
            "Freehand manual cutting (approximate fit), razor/scalpel": "Razor/scalpel (freehand)",
            "Manual cutting using ruler and razor/scalpel": "Razor/scalpel (ruled)",
            "Template-guided manual cutting": "Template",
        })
        .alias("ElectrodeCuttingMethod")
    )

    _pie_cols_2b = {
        "Experience": "Experience",
        "Electrode Pre-Treatment": "ElectrodePreTreatmentType",
        "Electrolyte Stirring": "ElectrolyteStirringPerformed",
        "Electrolyte Sparging": "ElectrolyteSparging",
        "Potentiostat Manufacturer": "PotentiostatManufacturer",
        "Electrode Cutting Method": "ElectrodeCuttingMethod",
        "Electrical Connection": "ElectricalConnectionScheme",
        "Test Temperature": "TestTemperature",
    }

    _pie_titles_2b = {
        "Experience": "Experience",
        "Electrode Pre-Treatment": ["Electrode", "Pre-Treatment"],
        "Electrolyte Stirring": ["Electrolyte", "Stirring"],
        "Electrolyte Sparging": ["Electrolyte", "Sparging"],
        "Potentiostat Manufacturer": ["Potentiostat", "Manufacturer"],
        "Electrode Cutting Method": ["Electrode", "Cutting Method"],
        "Electrical Connection": ["Electrical", "Connection"],
        "Test Temperature": ["Test", "Temperature"],
    }

    _exp_order = ["< 6 months", "6–12 months", "1–2 years", "2–5 years", "5–10 years", "10+ years"]
    _temp_order = ["≤18 °C", "18–20 °C", "20–22 °C", "22–24 °C", "24–26 °C", "Not recorded"]

    _charts_2b = []
    for _title, _col in _pie_cols_2b.items():
        _df_counts = (
            _df_meta_2b
            .filter(pl.col(_col) != "")
            .group_by(_col)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .to_pandas()
        )
        _sort = (
            _exp_order if _col == "Experience"
            else _temp_order if _col == "TestTemperature"
            else alt.Undefined
        )
        _display_title = _pie_titles_2b[_title]
        _chart = alt.Chart(_df_counts).mark_arc(stroke="white", strokeWidth=1).encode(
            theta=alt.Theta("count:Q"),
            color=alt.Color(f"{_col}:N",
                legend=alt.Legend(
                    orient="bottom",
                    columns=1,
                    title=None,
                    labelLimit=150,
                    symbolSize=80,
                    labelFontSize=11,
                    rowPadding=2,
                    offset=20,
                ),
                sort=_sort,
            ),
            tooltip=[alt.Tooltip(f"{_col}:N", title=_title), alt.Tooltip("count:Q", title="Count")]
        ).properties(
            title=alt.TitleParams(_display_title, anchor="middle"),
            width=150,
            height=150,
        )
        _charts_2b.append(_chart)

    _pie_chart_2b = alt.concat(*_charts_2b, columns=4, spacing=20).resolve_scale(color="independent")

    mo.Html(f'<div style="overflow-x:auto; text-align:center;">{mo.as_html(_pie_chart_2b).text}</div>')
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #000; margin: 40px 0 24px 0; padding-top: 16px;">
      <h2 style="color: #000; margin:0; font-size: 28px; font-weight: 700;">🔋 Charge-Discharge</h2>
      <p style="color: #000; margin: 4px 0 0 0;">Results from charge-discharge experiments across all participants and repeats.</p>
    </div>
    """)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">📈 Raw Charge-Discharge Curves</h3>
    </div>
    """)
    return


@app.cell
def _(df_metrics, mo, pl):
    raw_participant_selector_2a = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2a")["ParticipantID"].unique().to_list()),
        label="Phase 2a — Participants"
    )

    raw_repeat_selector_2a = mo.ui.multiselect(
        options=[1, 2, 3],
        value=[1],
        label="Phase 2a — Repeats"
    )

    raw_participant_selector_2b = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2b")["ParticipantID"].unique().to_list()),
        label="Phase 2b — Participants"
    )

    raw_repeat_selector_2b = mo.ui.multiselect(
        options=[1, 2, 3],
        value=[1],
        label="Phase 2b — Repeats"
    )

    raw_cycle_selector = mo.ui.dropdown(
        options=list(range(1, 13)),
        value=1,
        label="Cycle"
    )

    mo.vstack([
        mo.Html(f"""
        <div style="display:flex; justify-content:center; gap:64px; margin: 8px 0;">
            <div style="display:flex; flex-direction:column; gap:12px;">
                {mo.as_html(raw_participant_selector_2a).text}
                {mo.as_html(raw_repeat_selector_2a).text}
            </div>
            <div style="display:flex; flex-direction:column; gap:12px;">
                {mo.as_html(raw_participant_selector_2b).text}
                {mo.as_html(raw_repeat_selector_2b).text}
            </div>
        </div>
        """),
        mo.center(raw_cycle_selector),
    ], gap=1)
    return (
        raw_cycle_selector,
        raw_participant_selector_2a,
        raw_participant_selector_2b,
        raw_repeat_selector_2a,
        raw_repeat_selector_2b,
    )


@app.cell
def _(
    alt,
    client,
    mo,
    pl,
    raw_cycle_selector,
    raw_participant_selector_2a,
    raw_participant_selector_2b,
    raw_repeat_selector_2a,
    raw_repeat_selector_2b,
):
    _participants_2a = raw_participant_selector_2a.value
    _participants_2b = raw_participant_selector_2b.value
    _repeats_2a = raw_repeat_selector_2a.value
    _repeats_2b = raw_repeat_selector_2b.value
    _cycle = raw_cycle_selector.value
    if not _participants_2a and not _participants_2b:
        mo.stop(True, mo.center(mo.callout(mo.md("**Please select at least one participant to load charge-discharge data.**"), kind="warn")))
    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _selected = [("2a", p) for p in _participants_2a] + [("2b", p) for p in _participants_2b]
    _color_map_new = {f"{p} — {phase}": _palette[i % len(_palette)] for i, (phase, p) in enumerate(_selected)}
    _color_domain_new = list(_color_map_new.keys())
    _color_range_new = list(_color_map_new.values())
    _n_participants = len(_participants_2a) + len(_participants_2b)
    _bucket_size = 60 if _n_participants > 10 else 10
    def _query_phase(phase, participants, repeats):
        _plist = ", ".join(f"'{p}'" for p in participants)
        _repeat_filter = f"AND Repeat IN ({', '.join(str(r) for r in repeats)})" if repeats else ""
        return pl.from_arrow(
            client.query(f"""
                WITH bucketed AS (
                    SELECT ParticipantID, Repeat, Cycle, StepTime_s, Current_A, Potential_V, Index,
                           FLOOR(StepTime_s / {_bucket_size}) AS bucket,
                           ROW_NUMBER() OVER (
                               PARTITION BY ParticipantID, Repeat, CAST(Cycle AS INT64), Index, CAST(FLOOR(StepTime_s / {_bucket_size}) AS INT64)
                               ORDER BY ABS(StepTime_s - FLOOR(StepTime_s / {_bucket_size}) * {_bucket_size})
                           ) as rn
                    FROM `flow-battery-data-ingestion.electrochem.charge-discharge-data`
                    WHERE Cycle = {_cycle}
                    AND Phase = '{phase}'
                    AND ParticipantID IN ({_plist})
                    {_repeat_filter}
                    AND Cycle > 0
                    AND Index IN ('Chg', 'Dchg')
                )
                SELECT ParticipantID, Repeat, Cycle, StepTime_s, Current_A, Potential_V, Index
                FROM bucketed WHERE rn = 1
                ORDER BY ParticipantID, Repeat, Cycle, Index, StepTime_s
            """).to_arrow()
        ).with_columns(
            (pl.col("ParticipantID") + " — " + pl.lit(phase) + " R" + pl.col("Repeat").cast(pl.Utf8)).alias("Label"),
            (pl.col("ParticipantID") + " — " + pl.lit(phase)).alias("ParticipantKey"),
        )
    _frames = []
    if _participants_2a:
        _frames.append(_query_phase("2a", _participants_2a, _repeats_2a))
    if _participants_2b:
        _frames.append(_query_phase("2b", _participants_2b, _repeats_2b))
    _df_raw = pl.concat(_frames)
    alt.data_transformers.enable("default", max_rows=None)
    _chart = alt.Chart(_df_raw.to_pandas()).mark_line().encode(
        x=alt.X("StepTime_s:Q", title="Time (s)"),
        y=alt.Y("Potential_V:Q", title="Potential (V)"),
        color=alt.Color("ParticipantKey:N",
            scale=alt.Scale(domain=_color_domain_new, range=_color_range_new),
            title="Participant"),
        strokeDash=alt.StrokeDash("Index:N", title="Step"),
        detail="Label:N",
        tooltip=["ParticipantID", "Repeat", "Index", "StepTime_s", "Potential_V", "Current_A"]
    ).properties(
        width="container",
        height=400
    ).add_params(
        alt.selection_interval(bind="scales", zoom="wheel![event.shiftKey]")
    )
    mo.Html(f'<div style="overflow-x:hidden; width:100%;"><div style="max-width:900px; margin:0 auto;">{mo.as_html(_chart).text}</div></div>')
    return


@app.cell
def _(mo):
    mo.center(mo.Html("<p style='font-size:12px; color:#888;'>💡 Hold <b>Shift + scroll</b> to zoom · <b>Click</b> and <b>drag</b> to pan · <b>Double-click</b> to reset</p>"))
    return


@app.cell
def _(
    df_meta_2a,
    df_meta_2b,
    df_pol,
    mo,
    pl,
    raw_participant_selector_2a,
    raw_participant_selector_2b,
):
    _selected_2a = raw_participant_selector_2a.value
    _selected_2b = raw_participant_selector_2b.value
    _all_selected = [(p, "2a") for p in _selected_2a] + [(p, "2b") for p in _selected_2b]

    mo.stop(len(_all_selected) == 0)

    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _pid_color_map = {(p, phase): _palette[i % len(_palette)] for i, (p, phase) in enumerate(_all_selected)}

    _df_meta_2b_display = df_meta_2b.filter(
        pl.col("ConsentToPublicDisplay").str.to_lowercase().str.contains("yes")
    ).drop("ConsentToPublicDisplay")

    _col_display_names = {
        "Phase": "Phase",
        "JobRole": "Job Role",
        "Experience": "Experience",
        "ParticipantID": "Participant ID",
        "NumRepeats": "Number of Repeats",
        "ElectrodePreTreatmentType": "Electrode Pre-Treatment Type",
        "HeatTreatmentFurnaceType": "Heat Treatment Furnace Type",
        "HeatTreatmentFurnaceDetails": "Heat Treatment Furnace Details",
        "HeatTreatmentAtmosphere": "Heat Treatment Atmosphere",
        "HeatTreatmentRampRate": "Heat Treatment Ramp Rate (°C/min)",
        "HeatTreatmentDwellTemp": "Heat Treatment Dwell Temperature (°C)",
        "HeatTreatmentDwellTime": "Heat Treatment Dwell Time (h)",
        "HeatTreatmentCoolingMethod": "Heat Treatment Cooling Method",
        "HeatTreatmentBatching": "Heat Treatment Batching",
        "TimeBetweenHeatTreatmentAndUse": "Time Between Heat Treatment and Use (h)",
        "ElectrodeStorageAfterHeatTreatment": "Electrode Storage After Heat Treatment",
        "HeatTreatmentCoolingDetails": "Heat Treatment Cooling Details",
        "ElectrodeStorageAfterReceipt": "Electrode Storage After Receipt",
        "ElectrodeCuttingMethod": "Electrode Cutting and Sizing Method",
        "MembraneStorageType": "Membrane Storage Type",
        "LiquidForMembraneStorage": "Liquid for Membrane Storage",
        "MembraneStorageDetails": "Membrane Storage Details",
        "MembranePreTreatment": "Membrane Pre-Treatment",
        "NumMembranePreTreatmentSteps": "Number of Membrane Pre-Treatment Steps",
        "MembranePreTreatmentApplied": "Membrane Pre-Treatment Applied",
        "MembranePreTreatmentMedium": "Membrane Pre-Treatment Medium",
        "MembranePreTreatmentMediumConc": "Membrane Pre-Treatment Medium Concentration (M)",
        "MembranePreTreatmentDuration": "Membrane Pre-Treatment Duration (h)",
        "MembranePreTreatmentTemp": "Membrane Pre-Treatment Temperature (°C)",
        "AdditionalMembranePreTreatmentDetails": "Additional Membrane Pre-Treatment Details",
        "PotassiumFerrocyanideDetails": "Potassium Ferrocyanide Chemical Details",
        "PotassiumFerrocyanidePurity": "Potassium Ferrocyanide Purity (%)",
        "PotassiumFerrocyanideSupplier": "Potassium Ferrocyanide Supplier",
        "PotassiumFerricyanideIdentity": "Potassium Ferricyanide Chemical Identity",
        "PotassiumFerricyanidePurity": "Potassium Ferricyanide Purity (%)",
        "PotassiumFerricyanideSupplier": "Potassium Ferricyanide Supplier",
        "SupportingElectrolyteIdentity": "Supporting Electrolyte Chemical Identity",
        "PotassiumChloridePurity": "Potassium Chloride Purity (%)",
        "PotassiumChlorideSupplier": "Potassium Chloride Supplier",
        "WaterType": "Water Type",
        "WaterPurificationSystem": "Water Purification System",
        "WaterPurificationSystemDetails": "Water Purification System Details",
        "WaterResistivity": "Water Resistivity (MΩ·cm)",
        "ElectrolytePreparationStrategy": "Electrolyte Preparation Strategy",
        "ElectrolyteStockSize": "Electrolyte Stock/Batch Size (mL)",
        "ElectrolyteStorageEnvironment": "Electrolyte Storage Environment",
        "ElectrolyteStorageDuration": "Electrolyte Storage Duration (h)",
        "ElectrolyteVolumeMeasurementMethod": "Electrolyte Volume Measurement Method",
        "VolumeMeasurementInstrumentType": "Volume Measurement Instrument Type",
        "ReagentPurityUsedInCalculations": "Reagent Purity Used in Calculations",
        "ReagentPurityUsageDetails": "Reagent Purity Usage Details",
        "WaterPHMeasured": "Water pH Measured",
        "WaterPHDetails": "Water pH Details",
        "ElectrolytePHMeasured": "Electrolyte pH Measured",
        "ElectrolytePHValue": "Electrolyte pH Value",
        "PotentiostatUsage": "Potentiostat Usage",
        "PotentiostatManufacturer": "Potentiostat Manufacturer",
        "PotentiostatModel": "Potentiostat Model",
        "PotentiostatManufacturerCycling": "Potentiostat Manufacturer (Cycling)",
        "PotentiostatModelCycling": "Potentiostat Model (Cycling)",
        "PotentiostatManufacturerPolarisation": "Potentiostat Manufacturer (Polarisation)",
        "PotentiostatModelPolarisation": "Potentiostat Model (Polarisation)",
        "PotentiostatManufacturerImpedance": "Potentiostat Manufacturer (Impedance)",
        "PotentiostatModelImpedance": "Potentiostat Model (Impedance)",
        "PotentiostatCalibrationFrequency": "Potentiostat Calibration Frequency",
        "PotentiostatCalibrationMethod": "Potentiostat Calibration Method",
        "ConnectionMethodToCell": "Connection Method to Cell",
        "ElectricalConnectionScheme": "Electrical Connection Scheme",
        "PotentiostatCableLength": "Potentiostat Cable Length",
        "PotentiostatCableType": "Potentiostat Cable Type",
        "PotentiostatCableShielding": "Potentiostat Cable Shielding",
        "NumPumps": "Number of Pumps",
        "PumpType": "Pump Type",
        "PumpManufacturer": "Pump Manufacturer",
        "PumpModel": "Pump Model",
        "ReservoirMaterial": "Reservoir Material",
        "ReservoirType": "Reservoir Type",
        "ReservoirVolume": "Reservoir Volume (mL)",
        "TubingMaterial": "Tubing Material",
        "TubingManufacturer": "Tubing Manufacturer",
        "TubingInnerDiameter": "Tubing Inner Diameter",
        "WettedFittingMaterials": "Wetted Fitting Materials",
        "OtherWettedMaterials": "Other Wetted Materials",
        "ElectrolyteVolumeUsed": "Electrolyte Volume Used (mL)",
        "ImpedanceReservoirConfig": "Impedance Reservoir Configuration",
        "PolarisationReservoirConfig": "Polarisation Reservoir Configuration",
        "TemperatureControl": "Temperature Control",
        "TestTemperature": "Test Temperature",
        "PumpFlowRateCalibrated": "Pump Flow Rate Calibrated",
        "CalibrationPerformedWithCellInLine": "Calibration Performed With Cell In Line",
        "CalibrationFluidUsed": "Calibration Fluid Used",
        "PumpRecalibratedDuringExperiments": "Pump Recalibrated During Experiments",
        "PumpCalibrationMethod": "Pump Calibration Method",
        "LeakTestPerformed": "Leak Test Performed",
        "LeakTestDuration": "Leak Test Duration (h)",
        "LeakTestFlowRate": "Leak Test Flow Rate (mL/min)",
        "PreConditioningPerformed": "Pre-Conditioning/Break-in Period Performed",
        "BreakInPeriodDuration": "Break-in Period Duration (h)",
        "BreakInPeriodFlowRate": "Break-in Period Flow Rate (mL/min)",
        "CellAssemblyIssues": "Cell Assembly Issues",
        "AdditionalExperimentsPerformed": "Additional Experiments Performed",
        "AdditionalExperimentRationale": "Additional Experiment Rationale",
        "RepeatsTechniquesRepeated": "Repeat(s) and Technique(s) Repeated",
        "RepeatMethod": "Repeat Method",
        "TypicalGroupPracticeForLeakingCells": "Typical Group Practice for Leaking Cells",
        "ElectrolyteLeakage": "Electrolyte Leakage (Reported Data Only)",
        "LeakageDescription": "Leakage Description (Reported Data Only)",
        "LeakageSeverity": "Leakage Severity",
        "LeakageVolume": "Leakage Volume (mL)",
        "LeakageLocation": "Leakage Location",
        "ElectrolyteStirringPerformed": "Electrolyte Stirring Performed",
        "ReservoirStirringMethod": "Reservoir Stirring Method",
        "StirringRate": "Stirring Rate (RPM)",
        "ElectrolyteSparging": "Electrolyte Sparging Performed",
        "SpargingGas": "Sparging Gas",
        "GasHumidification": "Gas Humidification",
        "SpargingApplication": "Sparging Application",
        "SpargingGasFlowRate": "Sparging Gas Flow Rate",
        "PolarisationBeyond60mAcm2": "Polarisation Beyond ±60 mA/cm²",
        "MaxCurrentDensity": "Maximum Current Density Used (mA/cm²)",
        "RationaleForMaxCurrentDensity": "Rationale for Maximum Current Density",
        "ImpedanceControlMode": "Impedance Control Mode",
        "RationaleForImpedanceTechnique": "Rationale for Impedance Technique",
        "BasisForImpedanceParameters": "Basis for Selecting Impedance Parameters",
        "ReservoirTubingChanges": "Reservoir/Tubing Changes Observed During Cycling",
        "ObservableChangesAfterDisassembly": "Observable Changes After Cell Disassembly",
        "ComponentsAffected": "Components Affected",
        "NatureOfDiscolourationOrDeposits": "Nature of Discolouration or Deposits",
        "AdditionalTestsPerformed": "Additional Tests Performed",
        "AdditionalProtocolDetails": "Additional Protocol or Setup Details",
    }

    _sections = {
        "Participant Details": ["Phase", "JobRole", "Experience", "ParticipantID", "NumRepeats"],
        "Electrode Treatment & Handling": ["ElectrodePreTreatmentType", "HeatTreatmentFurnaceType", "HeatTreatmentFurnaceDetails", "HeatTreatmentAtmosphere", "HeatTreatmentRampRate", "HeatTreatmentDwellTemp", "HeatTreatmentDwellTime", "HeatTreatmentCoolingMethod", "HeatTreatmentBatching", "TimeBetweenHeatTreatmentAndUse", "ElectrodeStorageAfterHeatTreatment", "HeatTreatmentCoolingDetails", "ElectrodeStorageAfterReceipt", "ElectrodeCuttingMethod"],
        "Membrane Handling": ["MembraneStorageType", "LiquidForMembraneStorage", "MembraneStorageDetails", "MembranePreTreatment", "NumMembranePreTreatmentSteps", "MembranePreTreatmentApplied", "MembranePreTreatmentMedium", "MembranePreTreatmentMediumConc", "MembranePreTreatmentDuration", "MembranePreTreatmentTemp", "AdditionalMembranePreTreatmentDetails"],
        "Electrolyte Preparation": ["PotassiumFerrocyanideDetails", "PotassiumFerrocyanidePurity", "PotassiumFerrocyanideSupplier", "PotassiumFerricyanideIdentity", "PotassiumFerricyanidePurity", "PotassiumFerricyanideSupplier", "SupportingElectrolyteIdentity", "PotassiumChloridePurity", "PotassiumChlorideSupplier", "WaterType", "WaterPurificationSystem", "WaterPurificationSystemDetails", "WaterResistivity", "ElectrolytePreparationStrategy", "ElectrolyteStockSize", "ElectrolyteStorageEnvironment", "ElectrolyteStorageDuration", "ElectrolyteVolumeMeasurementMethod", "VolumeMeasurementInstrumentType", "ReagentPurityUsedInCalculations", "ReagentPurityUsageDetails", "WaterPHMeasured", "WaterPHDetails", "ElectrolytePHMeasured", "ElectrolytePHValue"],
        "Potentiostat Usage": ["PotentiostatUsage", "PotentiostatManufacturer", "PotentiostatModel", "PotentiostatManufacturerCycling", "PotentiostatModelCycling", "PotentiostatManufacturerPolarisation", "PotentiostatModelPolarisation", "PotentiostatManufacturerImpedance", "PotentiostatModelImpedance", "PotentiostatCalibrationFrequency", "PotentiostatCalibrationMethod", "ConnectionMethodToCell", "ElectricalConnectionScheme", "PotentiostatCableLength", "PotentiostatCableType", "PotentiostatCableShielding"],
        "Fluidics (Pumps, Tanks & Tubing)": ["NumPumps", "PumpType", "PumpManufacturer", "PumpModel", "ReservoirMaterial", "ReservoirType", "ReservoirVolume", "TubingMaterial", "TubingManufacturer", "TubingInnerDiameter", "WettedFittingMaterials", "OtherWettedMaterials"],
        "Test Protocols": ["TemperatureControl", "TestTemperature", "ElectrolyteVolumeUsed", "ImpedanceReservoirConfig", "PolarisationReservoirConfig", "PumpFlowRateCalibrated", "CalibrationPerformedWithCellInLine", "CalibrationFluidUsed", "PumpRecalibratedDuringExperiments", "PumpCalibrationMethod", "LeakTestPerformed", "LeakTestDuration", "LeakTestFlowRate", "PreConditioningPerformed", "BreakInPeriodDuration", "BreakInPeriodFlowRate", "CellAssemblyIssues", "AdditionalExperimentsPerformed", "AdditionalExperimentRationale", "RepeatsTechniquesRepeated", "RepeatMethod", "TypicalGroupPracticeForLeakingCells", "ElectrolyteLeakage", "LeakageDescription", "LeakageSeverity", "LeakageVolume", "LeakageLocation", "ElectrolyteStirringPerformed", "ReservoirStirringMethod", "StirringRate", "ElectrolyteSparging", "SpargingGas", "GasHumidification", "SpargingApplication", "SpargingGasFlowRate", "PolarisationBeyond60mAcm2", "MaxCurrentDensity", "RationaleForMaxCurrentDensity", "ImpedanceControlMode", "RationaleForImpedanceTechnique", "BasisForImpedanceParameters"],
        "Observations": ["ReservoirTubingChanges", "ObservableChangesAfterDisassembly", "ComponentsAffected", "NatureOfDiscolourationOrDeposits"],
        "Additional Information": ["AdditionalTestsPerformed", "AdditionalProtocolDetails"],
    }

    _row_height = 37
    _max_rows = 5
    _max_height = _row_height * _max_rows

    # Build Cell and Test Details table
    _df_flowrates_2a = (
        df_pol
        .filter(pl.col("Phase") == "2a")
        .select(["ParticipantID", "FlowRate"])
        .unique()
        .sort(["ParticipantID", "FlowRate"])
        .group_by("ParticipantID")
        .agg(pl.col("FlowRate").sort().cast(pl.Utf8).str.join(", ").alias("FlowRates"))
    )

    _cell_header_cols = ["Participant ID", "Cell", "Flow Field", "Area (cm²)", "Membrane", "Electrode", "Tank Volume (mL)", "Flow Rate(s) (mL/min)"]
    _cell_header_html = "".join(
        f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
        for h in _cell_header_cols
    )

    _cell_rows_html = ""
    for _pid, _phase in _all_selected:
        _color = _pid_color_map[(_pid, _phase)]
        _pid_cell = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
        </td>"""
        if _phase == "2a":
            _meta_row = df_meta_2a.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                continue
            _m = _meta_row.to_dicts()[0]
            _fr_row = _df_flowrates_2a.filter(pl.col("ParticipantID") == _pid)
            _fr = _fr_row["FlowRates"][0] if len(_fr_row) > 0 else "—"
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Cell", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("FF", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Area_cm2", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Membrane", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Electrode", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_fr}</td>
            </tr>"""
        else:
            _meta_row = _df_meta_2b_display.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                _cell_rows_html += f"""<tr>{_pid_cell}
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(_cell_header_cols) - 1}">Metadata not yet submitted for 2b</td>
                </tr>"""
                continue
            _m = _meta_row.to_dicts()[0]
            _tank = _m.get("ElectrolyteVolumeUsed", "—")
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">QUB</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">FTFF</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">16</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">N117</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">SGL GFD 4.65 EA</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_tank if _tank else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">25</td>
            </tr>"""

    _cell_table_html = f"""
    <div style="margin-bottom:16px;">
        <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">Cell and Test Details</div>
        <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
            <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                <thead><tr>{_cell_header_html}</tr></thead>
                <tbody>{_cell_rows_html}</tbody>
            </table>
        </div>
    </div>
    """ if _cell_rows_html else ""

    def _build_section_table(section_title, cols, all_selected, pid_color_map, df_2b):
        _header_cols = ["Participant ID"] + [_col_display_names.get(c, c) for c in cols]
        _header_html = "".join(
            f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
            for h in _header_cols
        )
        _rows_html = ""
        for _pid, _phase in all_selected:
            _color = pid_color_map[(_pid, _phase)]
            if _phase == "2a":
                _rows_html += f"""
                <tr>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                        <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                    </td>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Phase 2a metadata not yet formatted</td>
                </tr>
                """
            else:
                _row = df_2b.filter(pl.col("ParticipantID") == _pid)
                if len(_row) == 0:
                    _rows_html += f"""
                    <tr>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                        </td>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Metadata not yet submitted for 2b</td>
                    </tr>
                    """
                    continue
                _row_dict = _row.to_dicts()[0]
                _cells = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                    <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                </td>"""
                for _c in cols:
                    _v = _row_dict.get(_c, "")
                    _cells += f'<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap; max-width:300px; overflow:hidden; text-overflow:ellipsis;">{_v if _v else "—"}</td>'
                _rows_html += f"<tr>{_cells}</tr>"

        if not _rows_html:
            return ""

        return f"""
        <div style="margin-bottom:16px;">
            <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">{section_title}</div>
            <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
                <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                    <thead><tr>{_header_html}</tr></thead>
                    <tbody>{_rows_html}</tbody>
                </table>
            </div>
        </div>
        """

    _section_items = list(_sections.items())

    # Build all section tables
    _all_section_htmls = []
    for _title, _cols in _section_items:
        _all_section_htmls.append(_build_section_table(_title, _cols, _all_selected, _pid_color_map, _df_meta_2b_display))

    # Pair cell table with participant details, then rest in pairs
    _grid_cells = f"""
    <div style="min-width:0;">{_cell_table_html}</div>
    <div style="min-width:0;">{_all_section_htmls[0]}</div>
    """
    for _i in range(1, len(_all_section_htmls), 2):
        _left = _all_section_htmls[_i]
        _right = _all_section_htmls[_i + 1] if _i + 1 < len(_all_section_htmls) else ""
        _grid_cells += f"""
        <div style="min-width:0;">{_left}</div>
        <div style="min-width:0;">{_right}</div>
        """

    mo.stop(_grid_cells.strip() == "", mo.Html('<div style="margin-top:16px; color:#888; font-style:italic;">No metadata available for selected participants.</div>'))

    mo.Html(f"""
    <div style="margin-top:16px;">
        <div style="font-size:15px; font-weight:700; margin-bottom:16px;">Selected Participant Metadata</div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:24px; overflow:visible;">
            {_grid_cells}
        </div>
    </div>
    """)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">🔄 Per-Cycle Metrics</h3>
    </div>
    """)
    return


@app.cell
def _(df_metrics, mo, pl):
    metric_selector = mo.ui.dropdown(
        options={
            "Coulombic Efficiency (%)": "CoulombicEfficiency_pct",
            "Voltage Efficiency (%)": "VoltageEfficiency_pct",
            "Energy Efficiency (%)": "EnergyEfficiency_pct",
            "Charge Capacity (mAh)": "ChargeCapacity_mAh",
            "Discharge Capacity (mAh)": "DischargeCapacity_mAh",
            "Electrolyte Utilisation (%)": "ElectrolyteUtilisation_pct",
            "Capacity Decay (%/cycle)": "CapacityDecay_pct_per_cycle",
        },
        value="Coulombic Efficiency (%)",
        label="Metric",
    )

    participant_selector_2a = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2a")["ParticipantID"].unique().to_list()),
        label="Phase 2a — Participants",
    )

    repeat_selector_2a = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2a")["Repeat"].unique().to_list()),
        label="Phase 2a — Repeats",
    )

    participant_selector_2b = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2b")["ParticipantID"].unique().to_list()),
        label="Phase 2b — Participants",
    )

    repeat_selector_2b = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2b")["Repeat"].unique().to_list()),
        label="Phase 2b — Repeats",
    )

    mo.vstack([
        mo.center(metric_selector),
        mo.Html(f"""
        <div style="display:flex; justify-content:center; gap:64px; margin: 8px 0;">
            <div style="display:flex; flex-direction:column; gap:12px;">
                {mo.as_html(participant_selector_2a).text}
                {mo.as_html(repeat_selector_2a).text}
            </div>
            <div style="display:flex; flex-direction:column; gap:12px;">
                {mo.as_html(participant_selector_2b).text}
                {mo.as_html(repeat_selector_2b).text}
            </div>
        </div>
        """),
    ], gap=1)
    return (
        metric_selector,
        participant_selector_2a,
        participant_selector_2b,
        repeat_selector_2a,
        repeat_selector_2b,
    )


@app.cell
def _(
    alt,
    df_metrics,
    metric_selector,
    mo,
    participant_selector_2a,
    participant_selector_2b,
    pl,
    repeat_selector_2a,
    repeat_selector_2b,
):
    _metric = metric_selector.value
    _participants_2a = participant_selector_2a.value
    _participants_2b = participant_selector_2b.value
    _repeats_2a = repeat_selector_2a.value
    _repeats_2b = repeat_selector_2b.value

    if not _participants_2a and not _participants_2b:
        mo.stop(True, mo.center(mo.callout(mo.md("**Please select at least one participant to load per-cycle metrics.**"), kind="warn")))

    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]

    _df_2a = df_metrics.filter(
        (pl.col("Phase") == "2a") &
        (pl.col("IsOutlier") == False) &
        (pl.col("CycleNumber") >= 1) &
        (pl.col("CycleNumber") <= 12)
    )
    _df_2b = df_metrics.filter(
        (pl.col("Phase") == "2b") &
        (pl.col("IsOutlier") == False) &
        (pl.col("CycleNumber") >= 1) &
        (pl.col("CycleNumber") <= 12)
    )

    if _participants_2a:
        _df_2a = _df_2a.filter(pl.col("ParticipantID").is_in(_participants_2a))
    else:
        _df_2a = _df_2a.clear()

    if _repeats_2a:
        _df_2a = _df_2a.filter(pl.col("Repeat").is_in(_repeats_2a))

    if _participants_2b:
        _df_2b = _df_2b.filter(pl.col("ParticipantID").is_in(_participants_2b))
    else:
        _df_2b = _df_2b.clear()

    if _repeats_2b:
        _df_2b = _df_2b.filter(pl.col("Repeat").is_in(_repeats_2b))

    _df_2a = _df_2a.with_columns(
        (pl.col("ParticipantID") + " — 2a").alias("ParticipantKey")
    )
    _df_2b = _df_2b.with_columns(
        (pl.col("ParticipantID") + " — 2b").alias("ParticipantKey")
    )

    _df_plot = pl.concat([_df_2a, _df_2b])

    if len(_df_plot) == 0:
        mo.stop(True, mo.center(mo.callout(mo.md("**No data matches the current selection.**"), kind="warn")))

    _df_plot = _df_plot.with_columns(
        pl.col("CycleNumber").cast(pl.Int64).alias("CycleNumber"),
        pl.col("Repeat").cast(pl.Int64).alias("Repeat"),
    )
    _df_plot = _df_plot.with_columns(
        (pl.col("ParticipantKey") + " R" + pl.col("Repeat").cast(pl.Utf8)).alias("RepeatKey")
    ).sort(["RepeatKey", "CycleNumber"])

    _all_keys = sorted(_df_plot["ParticipantKey"].unique().to_list())
    _color_map = {k: _palette[i % len(_palette)] for i, k in enumerate(_all_keys)}
    _color_domain = list(_color_map.keys())
    _color_range = [_color_map[k] for k in _color_domain]

    metric_col = _metric
    metric_label = [k for k, v in metric_selector.options.items() if v == metric_col][0]

    _min = float(_df_plot[metric_col].min())
    _max = float(_df_plot[metric_col].max())
    _pad = (_max - _min) * 0.1

    _base = alt.Chart(_df_plot.to_pandas())

    _lines = _base.mark_line().encode(
        x=alt.X("CycleNumber:O", title="Cycle Number"),
        y=alt.Y(f"{metric_col}:Q", title=metric_label, scale=alt.Scale(domain=[_min - _pad, _max + _pad])),
        color=alt.Color("ParticipantKey:N",
            scale=alt.Scale(domain=_color_domain, range=_color_range),
            legend=alt.Legend(orient="right", titleFontSize=13, labelFontSize=12, title="Participant")),
        strokeDash=alt.StrokeDash("Repeat:O",
            scale=alt.Scale(range=[[1,0], [6,3], [2,4]]),
            legend=None),
        detail="RepeatKey:N",
        order=alt.Order("CycleNumber:O"),
    )

    _points = _base.mark_point(filled=True, size=60).encode(
       x=alt.X("CycleNumber:O", title="Cycle Number", axis=alt.Axis(labelAngle=0, labelAlign="center", labelPadding=10)),
        y=alt.Y(f"{metric_col}:Q"),
        color=alt.Color("ParticipantKey:N",
            scale=alt.Scale(domain=_color_domain, range=_color_range),
            legend=None),
        shape=alt.Shape("Repeat:O",
            scale=alt.Scale(range=["circle", "square", "triangle-up"]),
            legend=None),
        size=alt.condition(
            alt.datum["Repeat"] == 3,
            alt.value(150),
            alt.value(60)
        ),
        detail="RepeatKey:N",
        order=alt.Order("CycleNumber:O"),
        tooltip=["ParticipantKey", "Repeat", "CycleNumber", metric_col]
    )

    chart = (_lines + _points).properties(
        width="container",
        height=400
    ).add_params(
        alt.selection_interval(bind="scales", zoom="wheel![event.shiftKey]")
    )

    _repeat_legend = mo.Html("""
    <div style="display:flex; justify-content:center; gap:32px; margin-bottom:8px;">
        <div style="display:flex; align-items:center; gap:8px; font-size:14px; font-family:Open Sans;">
            <svg width="16" height="16"><circle cx="8" cy="8" r="6" fill="#555"/></svg>
            <span>Repeat 1</span>
        </div>
        <div style="display:flex; align-items:center; gap:8px; font-size:14px; font-family:Open Sans;">
            <svg width="16" height="16"><rect x="2" y="2" width="12" height="12" fill="#555"/></svg>
            <span>Repeat 2</span>
        </div>
        <div style="display:flex; align-items:center; gap:8px; font-size:14px; font-family:Open Sans;">
            <svg width="18" height="18"><polygon points="9,1 17,17 1,17" fill="#555"/></svg>
            <span>Repeat 3</span>
        </div>
    </div>
    """)

    mo.vstack([
        _repeat_legend,
        mo.Html(f'<div style="overflow-x:hidden; width:100%;"><div style="max-width:800px; margin:0 auto;">{mo.as_html(chart).text}</div></div>')
    ], gap=1)
    return


@app.cell
def _(mo):
    mo.center(mo.Html("<p style='font-size:12px; color:#888;'>💡 Hold <b>Shift + scroll</b> to zoom · <b>Click</b> and <b>drag</b> to pan · <b>Double-click</b> to reset</p>"))
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #ddd; margin: 24px 0 12px 0; padding-top: 12px;">
      <h3 style="color: #000; margin:0; font-size: 18px; font-weight: 700;">👥 Average Metrics by Participant</h3>
    </div>
    """)
    return


@app.cell
def _(df_metrics, mo, pl):
    avg_participant_selector_2a = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2a")["ParticipantID"].unique().to_list()),
        label="Phase 2a — Participants",
    )

    avg_participant_selector_2b = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2b")["ParticipantID"].unique().to_list()),
        label="Phase 2b — Participants",
    )

    mo.Html(f"""
    <div style="display:flex; justify-content:center; gap:64px; margin: 8px 0;">
        <div style="display:flex; flex-direction:column; gap:12px;">
            {mo.as_html(avg_participant_selector_2a).text}
        </div>
        <div style="display:flex; flex-direction:column; gap:12px;">
            {mo.as_html(avg_participant_selector_2b).text}
        </div>
    </div>
    """)
    return avg_participant_selector_2a, avg_participant_selector_2b


@app.cell
def _(
    alt,
    avg_participant_selector_2a,
    avg_participant_selector_2b,
    df_metrics,
    mo,
    pl,
):
    _selected_2a = avg_participant_selector_2a.value
    _selected_2b = avg_participant_selector_2b.value

    if not _selected_2a and not _selected_2b:
        mo.stop(True, mo.center(mo.callout(mo.md("**Please select at least one participant.**"), kind="warn")))

    _df_2a = df_metrics.filter(
        (pl.col("Phase") == "2a") & (pl.col("IsOutlier") == False)
    )
    _df_2b = df_metrics.filter(
        (pl.col("Phase") == "2b") & (pl.col("IsOutlier") == False)
    )

    if _selected_2a:
        _df_2a = _df_2a.filter(pl.col("ParticipantID").is_in(_selected_2a))
    else:
        _df_2a = _df_2a.clear()

    if _selected_2b:
        _df_2b = _df_2b.filter(pl.col("ParticipantID").is_in(_selected_2b))
    else:
        _df_2b = _df_2b.clear()

    _df_2a = _df_2a.with_columns(
        (pl.col("ParticipantID") + " — 2a").alias("ParticipantKey")
    )
    _df_2b = _df_2b.with_columns(
        (pl.col("ParticipantID") + " — 2b").alias("ParticipantKey")
    )

    _df_avg = pl.concat([_df_2a, _df_2b])

    _metrics_map = {
        "Coulombic Efficiency (%)": "CoulombicEfficiency_pct",
        "Voltage Efficiency (%)": "VoltageEfficiency_pct",
        "Energy Efficiency (%)": "EnergyEfficiency_pct",
        "Electrolyte Utilisation (%)": "ElectrolyteUtilisation_pct",
        "Discharge Capacity (mAh)": "DischargeCapacity_mAh",
    }

    _participants = sorted(_df_avg["ParticipantKey"].unique().to_list())

    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
        "#EAB308", "#64748B", "#DC2626", "#7C3AED", "#059669",
        "#D97706", "#2563EB", "#DB2777", "#0D9488", "#CA8A04",
        "#4F46E5", "#BE185D", "#0F766E", "#B45309", "#1D4ED8",
        "#9333EA", "#15803D", "#C2410C", "#1E40AF", "#6D28D9",
    ]
    _participant_colors = {p: _palette[i % len(_palette)] for i, p in enumerate(_participants)}

    _rows = []
    for _label, _col in _metrics_map.items():
        for _pid in _participants:
            _pdf = _df_avg.filter(pl.col("ParticipantKey") == _pid)
            if len(_pdf) > 0:
                _rows.append({
                    "Metric": _label,
                    "ParticipantKey": _pid,
                    "Mean (%)": round(_pdf[_col].mean(), 2),
                    "StdDev": round(_pdf[_col].std(), 2),
                    "Color": _participant_colors[_pid],
                })

    _df_plot = pl.DataFrame(_rows).to_pandas()

    _sd_ranges = {}
    for _label, _col in _metrics_map.items():
        _sds = [r["StdDev"] for r in _rows if r["Metric"] == _label]
        _sd_ranges[_label] = (min(_sds), max(_sds))

    def _sd_color(sd, min_sd, max_sd):
        if max_sd == min_sd:
            t = 0.0
        else:
            t = (sd - min_sd) / (max_sd - min_sd)
        if t < 0.5:
            r = int(255 * (t * 2))
            g = 200
        else:
            r = 255
            g = int(200 * (1 - (t - 0.5) * 2))
        return f"rgb({r},{g},100)"

    _chart_elements = []
    for _label, _col in _metrics_map.items():
        _data = _df_plot[_df_plot["Metric"] == _label].copy()
        _n = len(_data)
        _bar_width = min(40, max(10, int(600 / _n) - 10))
        _ymin = float((_data["Mean (%)"] - _data["StdDev"]).min())
        _ymax = float((_data["Mean (%)"] + _data["StdDev"]).max())
        _pad = (_ymax - _ymin) * 0.1
        _ylo = max(0, _ymin - _pad)
        _yhi = _ymax + _pad
        _data["ybase"] = _ylo
        _ytitle = "Mean (mAh)" if "mAh" in _label else "Mean (%)"
        _base = alt.Chart(_data).properties(width="container", height=300)
        _bars = _base.mark_bar(width=_bar_width).encode(
            x=alt.X("ParticipantKey:N", title=None, axis=alt.Axis(labelAngle=-45), sort=_participants),
            y=alt.Y("ybase:Q", scale=alt.Scale(domain=[_ylo, _yhi], zero=False), title=_ytitle),
            y2=alt.Y2("Mean (%):Q"),
            color=alt.Color("Color:N", scale=None, legend=None),
            tooltip=["ParticipantKey", "Mean (%)", "StdDev"]
        )
        _errors = _base.mark_errorbar(color="black", ticks=True, thickness=1, size=5).encode(
            x=alt.X("ParticipantKey:N", title=None, sort=_participants),
            y=alt.Y("Mean (%):Q", scale=alt.Scale(domain=[_ylo, _yhi], zero=False), title=_ytitle),
            yError=alt.YError("StdDev:Q"),
        )
        _c = (_bars + _errors).properties(title=_label)
        _chart_elements.append(mo.Html(f'<div style="overflow-x:hidden; width:100%;"><div style="max-width:800px; margin:0 auto;">{mo.as_html(_c).text}</div></div>'))

    _table_rows = []
    for _pid in _participants:
        _swatch = f'<div style="width:12px;height:12px;border-radius:50%;background:{_participant_colors[_pid]};display:inline-block;vertical-align:middle;margin-right:6px;"></div>'
        _row_cells = [f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0; font-weight:600; text-align:center; min-width:160px; white-space:nowrap;">{_swatch}{_pid}</td>']
        for _label, _col in _metrics_map.items():
            _pdf = _df_avg.filter(pl.col("ParticipantKey") == _pid)
            if len(_pdf) > 0:
                _mean = round(_pdf[_col].mean(), 2)
                _sd = round(_pdf[_col].std(), 2)
                _min_sd, _max_sd = _sd_ranges[_label]
                _bg = _sd_color(_sd, _min_sd, _max_sd)
                _row_cells.append(
                    f'<td style="padding:6px 14px; border-bottom:1px solid #f0f0f0; background:{_bg}; text-align:center;">'
                    f'{_mean} ± {_sd}</td>'
                )
        _table_rows.append("<tr>" + "".join(_row_cells) + "</tr>")

    _headers = ['<th style="padding:6px 14px; text-align:center; border-bottom:2px solid #e5e7eb; min-width:160px;">Participant</th>']
    _headers += [
        f'<th style="padding:6px 14px; text-align:center; border-bottom:2px solid #e5e7eb;">{h}</th>'
        for h in _metrics_map.keys()
    ]

    _table_key = mo.Html("""
    <div style="display:flex; justify-content:center; align-items:center; gap:8px; margin-bottom:8px; font-size:13px; font-family:Open Sans;">
        <span style="color:#000; font-weight:400;">Standard Deviation:</span>
        <span style="color:#000; font-weight:700;">Low</span>
        <div style="width:120px; height:16px; border-radius:4px; background:linear-gradient(to right, rgb(0,200,100), rgb(255,200,100), rgb(255,0,100));"></div>
        <span style="color:#000; font-weight:700;">High</span>
    </div>
    """)

    _chart_elements += [
        _table_key,
        mo.Html(f"""
        <div style="overflow-x:auto;">
        <table style="border-collapse:collapse; font-size:14px;">
          <thead><tr>{"".join(_headers)}</tr></thead>
          <tbody>{"".join(_table_rows)}</tbody>
        </table>
        </div>
        """)
    ]

    mo.vstack(_chart_elements)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #000; margin: 40px 0 24px 0; padding-top: 16px;">
      <h2 style="color: #000; margin:0; font-size: 28px; font-weight: 700;">🔌 Polarisation</h2>
      <p style="color: #000; margin: 4px 0 0 0;">Results from polarisation experiments across all participants and repeats.</p>
    </div>
    """)
    return


@app.cell
def _(client, mo, pl):
    df_pol = pl.from_arrow(
        client.query("""
            SELECT Phase, ParticipantID, FlowRate, Repeat,
                   PulseNumber, PulseTime_s, AvgCurrent_mA, AvgVoltage_V
            FROM `flow-battery-data-ingestion.electrochem.polarisation-metrics`
            WHERE RowType = 'pulse'
            AND Phase IN ('2a', '2b')
        """).to_arrow()
    )
    mo.md(f"Loaded **{len(df_pol):,}** polarisation rows")
    None
    return (df_pol,)


@app.cell
def _(df_pol, mo, pl):
    pol_participant_selector_2a = mo.ui.multiselect(
        options=sorted(df_pol.filter(pl.col("Phase") == "2a")["ParticipantID"].unique().to_list()),
        label="Phase 2a — Participants"
    )
    pol_repeat_selector_2a = mo.ui.multiselect(
        options=sorted(df_pol.filter(pl.col("Phase") == "2a")["Repeat"].unique().to_list()),
        label="Phase 2a — Repeats"
    )
    pol_participant_selector_2b = mo.ui.multiselect(
        options=sorted(df_pol.filter(pl.col("Phase") == "2b")["ParticipantID"].unique().to_list()),
        label="Phase 2b — Participants"
    )
    pol_repeat_selector_2b = mo.ui.multiselect(
        options=sorted(df_pol.filter(pl.col("Phase") == "2b")["Repeat"].unique().to_list()),
        label="Phase 2b — Repeats"
    )

    mo.Html(f"""
    <div style="display:flex; justify-content:center; gap:64px; margin: 8px 0;">
        <div style="display:flex; flex-direction:column; gap:30px; width:280px;">
            {mo.as_html(pol_participant_selector_2a).text}
            {mo.as_html(pol_repeat_selector_2a).text}
        </div>
        <div style="display:flex; flex-direction:column; gap:30px; width:280px;">
            {mo.as_html(pol_participant_selector_2b).text}
            {mo.as_html(pol_repeat_selector_2b).text}
        </div>
    </div>
    """)
    return (
        pol_participant_selector_2a,
        pol_participant_selector_2b,
        pol_repeat_selector_2a,
        pol_repeat_selector_2b,
    )


@app.cell
def _(
    df_pol,
    mo,
    pl,
    pol_participant_selector_2a,
    pol_participant_selector_2b,
):
    _pol_available_flowrates = (
        df_pol.filter(
            (pl.col("Phase") == "2a") &
            (pl.col("ParticipantID").is_in(pol_participant_selector_2a.value))
        ) if pol_participant_selector_2a.value else df_pol.filter(pl.col("Phase") == "2a")
    )["FlowRate"].unique().sort().to_list()

    pol_flowrate_selector = mo.ui.multiselect(
        options=_pol_available_flowrates,
        label="Phase 2a — Flow Rates"
    )

    mo.Html(f"""
    <div style="display:flex; justify-content:center; gap:65px; margin: 8px 0;">
        <div style="display:flex; flex-direction:column; gap:20px; width:280px;">
            {mo.as_html(pol_flowrate_selector).text}
        </div>
        <div style="visibility:hidden; display:flex; flex-direction:column; gap:20px; width:280px;">
            {mo.as_html(pol_participant_selector_2b).text}
        </div>
    </div>
    """)
    return (pol_flowrate_selector,)


@app.cell
def _(
    alt,
    client,
    mo,
    pl,
    pol_flowrate_selector,
    pol_participant_selector_2a,
    pol_participant_selector_2b,
    pol_repeat_selector_2a,
    pol_repeat_selector_2b,
):
    _pol_participants_2a = pol_participant_selector_2a.value
    _pol_participants_2b = pol_participant_selector_2b.value
    _pol_repeats_2a = pol_repeat_selector_2a.value
    _pol_repeats_2b = pol_repeat_selector_2b.value
    _pol_flowrates = pol_flowrate_selector.value
    if not _pol_participants_2a and not _pol_participants_2b:
        mo.stop(True, mo.center(mo.callout(mo.md("**Please select at least one participant to load polarisation data.**"), kind="warn")))
    _flowrate_filter = f"AND FlowRate IN ({', '.join(str(f) for f in _pol_flowrates)})" if _pol_flowrates else ""
    def _query_pol(phase, participants, repeats):
        _plist = ", ".join(f"'{p}'" for p in participants)
        _repeat_filter = f"AND Repeat IN ({', '.join(str(r) for r in repeats)})" if repeats else ""
        _fr_filter = _flowrate_filter if phase == "2a" else ""
        return pl.from_arrow(
            client.query(f"""
                SELECT ParticipantID, FlowRate, Repeat,
                       PulseNumber, PulseTime_s, AvgCurrent_mA, AvgVoltage_V
                FROM `flow-battery-data-ingestion.electrochem.polarisation-metrics`
                WHERE RowType = 'pulse'
                AND Phase = '{phase}'
                AND ParticipantID IN ({_plist})
                {_repeat_filter}
                {_fr_filter}
                ORDER BY ParticipantID, Repeat, FlowRate, PulseNumber
            """).to_arrow()
        ).with_columns(
            (pl.col("ParticipantID") + " — " + pl.lit(phase) + " R" + pl.col("Repeat").cast(pl.Utf8) + " " + pl.col("FlowRate").cast(pl.Utf8) + " mL/min").alias("Label"),
            (pl.col("ParticipantID") + " — " + pl.lit(phase)).alias("ParticipantKey"),
        )
    _frames = []
    if _pol_participants_2a:
        _frames.append(_query_pol("2a", _pol_participants_2a, _pol_repeats_2a))
    if _pol_participants_2b:
        _frames.append(_query_pol("2b", _pol_participants_2b, _pol_repeats_2b))
    _df_pol_plot = pl.concat(_frames)
    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _selected = [("2a", p) for p in _pol_participants_2a] + [("2b", p) for p in _pol_participants_2b]
    _color_map = {f"{p} — {phase}": _palette[i % len(_palette)] for i, (phase, p) in enumerate(_selected)}
    _color_domain = list(_color_map.keys())
    _color_range = list(_color_map.values())
    alt.data_transformers.enable("default", max_rows=None)
    _pol_chart = alt.Chart(_df_pol_plot.to_pandas()).mark_point().encode(
        x=alt.X("AvgCurrent_mA:Q", title="Current (mA)"),
        y=alt.Y("AvgVoltage_V:Q", title="Voltage (V)"),
        color=alt.Color("ParticipantKey:N",
            scale=alt.Scale(domain=_color_domain, range=_color_range),
            title="Participant"),
        detail="Label:N",
        tooltip=["ParticipantID", "Repeat", "FlowRate", "PulseNumber", "AvgCurrent_mA", "AvgVoltage_V"]
    ).properties(
        width="container",
        height=400
    ).add_params(
        alt.selection_interval(bind="scales", zoom="wheel![event.shiftKey]")
    )
    mo.Html(f'<div style="overflow-x:hidden; width:100%;"><div style="max-width:800px; margin:0 auto;">{mo.as_html(_pol_chart).text}</div></div>')
    return


@app.cell
def _(mo):
    mo.center(mo.Html("<p style='font-size:12px; color:#888;'>💡 Hold <b>Shift + scroll</b> to zoom · <b>Click</b> and <b>drag</b> to pan · <b>Double-click</b> to reset</p>"))
    return


@app.cell
def _(
    df_meta_2a,
    df_meta_2b,
    df_pol,
    mo,
    pl,
    pol_participant_selector_2a,
    pol_participant_selector_2b,
):
    _selected_2a = pol_participant_selector_2a.value
    _selected_2b = pol_participant_selector_2b.value
    _all_selected = [(p, "2a") for p in _selected_2a] + [(p, "2b") for p in _selected_2b]

    mo.stop(len(_all_selected) == 0)

    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _pid_color_map = {(p, phase): _palette[i % len(_palette)] for i, (p, phase) in enumerate(_all_selected)}

    _df_meta_2b_display = df_meta_2b.filter(
        pl.col("ConsentToPublicDisplay").str.to_lowercase().str.contains("yes")
    ).drop("ConsentToPublicDisplay")

    _col_display_names = {
        "Phase": "Phase",
        "JobRole": "Job Role",
        "Experience": "Experience",
        "ParticipantID": "Participant ID",
        "NumRepeats": "Number of Repeats",
        "ElectrodePreTreatmentType": "Electrode Pre-Treatment Type",
        "HeatTreatmentFurnaceType": "Heat Treatment Furnace Type",
        "HeatTreatmentFurnaceDetails": "Heat Treatment Furnace Details",
        "HeatTreatmentAtmosphere": "Heat Treatment Atmosphere",
        "HeatTreatmentRampRate": "Heat Treatment Ramp Rate (°C/min)",
        "HeatTreatmentDwellTemp": "Heat Treatment Dwell Temperature (°C)",
        "HeatTreatmentDwellTime": "Heat Treatment Dwell Time (h)",
        "HeatTreatmentCoolingMethod": "Heat Treatment Cooling Method",
        "HeatTreatmentBatching": "Heat Treatment Batching",
        "TimeBetweenHeatTreatmentAndUse": "Time Between Heat Treatment and Use (h)",
        "ElectrodeStorageAfterHeatTreatment": "Electrode Storage After Heat Treatment",
        "HeatTreatmentCoolingDetails": "Heat Treatment Cooling Details",
        "ElectrodeStorageAfterReceipt": "Electrode Storage After Receipt",
        "ElectrodeCuttingMethod": "Electrode Cutting and Sizing Method",
        "MembraneStorageType": "Membrane Storage Type",
        "LiquidForMembraneStorage": "Liquid for Membrane Storage",
        "MembraneStorageDetails": "Membrane Storage Details",
        "MembranePreTreatment": "Membrane Pre-Treatment",
        "NumMembranePreTreatmentSteps": "Number of Membrane Pre-Treatment Steps",
        "MembranePreTreatmentApplied": "Membrane Pre-Treatment Applied",
        "MembranePreTreatmentMedium": "Membrane Pre-Treatment Medium",
        "MembranePreTreatmentMediumConc": "Membrane Pre-Treatment Medium Concentration (M)",
        "MembranePreTreatmentDuration": "Membrane Pre-Treatment Duration (h)",
        "MembranePreTreatmentTemp": "Membrane Pre-Treatment Temperature (°C)",
        "AdditionalMembranePreTreatmentDetails": "Additional Membrane Pre-Treatment Details",
        "PotassiumFerrocyanideDetails": "Potassium Ferrocyanide Chemical Details",
        "PotassiumFerrocyanidePurity": "Potassium Ferrocyanide Purity (%)",
        "PotassiumFerrocyanideSupplier": "Potassium Ferrocyanide Supplier",
        "PotassiumFerricyanideIdentity": "Potassium Ferricyanide Chemical Identity",
        "PotassiumFerricyanidePurity": "Potassium Ferricyanide Purity (%)",
        "PotassiumFerricyanideSupplier": "Potassium Ferricyanide Supplier",
        "SupportingElectrolyteIdentity": "Supporting Electrolyte Chemical Identity",
        "PotassiumChloridePurity": "Potassium Chloride Purity (%)",
        "PotassiumChlorideSupplier": "Potassium Chloride Supplier",
        "WaterType": "Water Type",
        "WaterPurificationSystem": "Water Purification System",
        "WaterPurificationSystemDetails": "Water Purification System Details",
        "WaterResistivity": "Water Resistivity (MΩ·cm)",
        "ElectrolytePreparationStrategy": "Electrolyte Preparation Strategy",
        "ElectrolyteStockSize": "Electrolyte Stock/Batch Size (mL)",
        "ElectrolyteStorageEnvironment": "Electrolyte Storage Environment",
        "ElectrolyteStorageDuration": "Electrolyte Storage Duration (h)",
        "ElectrolyteVolumeMeasurementMethod": "Electrolyte Volume Measurement Method",
        "VolumeMeasurementInstrumentType": "Volume Measurement Instrument Type",
        "ReagentPurityUsedInCalculations": "Reagent Purity Used in Calculations",
        "ReagentPurityUsageDetails": "Reagent Purity Usage Details",
        "WaterPHMeasured": "Water pH Measured",
        "WaterPHDetails": "Water pH Details",
        "ElectrolytePHMeasured": "Electrolyte pH Measured",
        "ElectrolytePHValue": "Electrolyte pH Value",
        "PotentiostatUsage": "Potentiostat Usage",
        "PotentiostatManufacturer": "Potentiostat Manufacturer",
        "PotentiostatModel": "Potentiostat Model",
        "PotentiostatManufacturerCycling": "Potentiostat Manufacturer (Cycling)",
        "PotentiostatModelCycling": "Potentiostat Model (Cycling)",
        "PotentiostatManufacturerPolarisation": "Potentiostat Manufacturer (Polarisation)",
        "PotentiostatModelPolarisation": "Potentiostat Model (Polarisation)",
        "PotentiostatManufacturerImpedance": "Potentiostat Manufacturer (Impedance)",
        "PotentiostatModelImpedance": "Potentiostat Model (Impedance)",
        "PotentiostatCalibrationFrequency": "Potentiostat Calibration Frequency",
        "PotentiostatCalibrationMethod": "Potentiostat Calibration Method",
        "ConnectionMethodToCell": "Connection Method to Cell",
        "ElectricalConnectionScheme": "Electrical Connection Scheme",
        "PotentiostatCableLength": "Potentiostat Cable Length",
        "PotentiostatCableType": "Potentiostat Cable Type",
        "PotentiostatCableShielding": "Potentiostat Cable Shielding",
        "NumPumps": "Number of Pumps",
        "PumpType": "Pump Type",
        "PumpManufacturer": "Pump Manufacturer",
        "PumpModel": "Pump Model",
        "ReservoirMaterial": "Reservoir Material",
        "ReservoirType": "Reservoir Type",
        "ReservoirVolume": "Reservoir Volume (mL)",
        "TubingMaterial": "Tubing Material",
        "TubingManufacturer": "Tubing Manufacturer",
        "TubingInnerDiameter": "Tubing Inner Diameter",
        "WettedFittingMaterials": "Wetted Fitting Materials",
        "OtherWettedMaterials": "Other Wetted Materials",
        "ElectrolyteVolumeUsed": "Electrolyte Volume Used (mL)",
        "ImpedanceReservoirConfig": "Impedance Reservoir Configuration",
        "PolarisationReservoirConfig": "Polarisation Reservoir Configuration",
        "TemperatureControl": "Temperature Control",
        "TestTemperature": "Test Temperature",
        "PumpFlowRateCalibrated": "Pump Flow Rate Calibrated",
        "CalibrationPerformedWithCellInLine": "Calibration Performed With Cell In Line",
        "CalibrationFluidUsed": "Calibration Fluid Used",
        "PumpRecalibratedDuringExperiments": "Pump Recalibrated During Experiments",
        "PumpCalibrationMethod": "Pump Calibration Method",
        "LeakTestPerformed": "Leak Test Performed",
        "LeakTestDuration": "Leak Test Duration (h)",
        "LeakTestFlowRate": "Leak Test Flow Rate (mL/min)",
        "PreConditioningPerformed": "Pre-Conditioning/Break-in Period Performed",
        "BreakInPeriodDuration": "Break-in Period Duration (h)",
        "BreakInPeriodFlowRate": "Break-in Period Flow Rate (mL/min)",
        "CellAssemblyIssues": "Cell Assembly Issues",
        "AdditionalExperimentsPerformed": "Additional Experiments Performed",
        "AdditionalExperimentRationale": "Additional Experiment Rationale",
        "RepeatsTechniquesRepeated": "Repeat(s) and Technique(s) Repeated",
        "RepeatMethod": "Repeat Method",
        "TypicalGroupPracticeForLeakingCells": "Typical Group Practice for Leaking Cells",
        "ElectrolyteLeakage": "Electrolyte Leakage (Reported Data Only)",
        "LeakageDescription": "Leakage Description (Reported Data Only)",
        "LeakageSeverity": "Leakage Severity",
        "LeakageVolume": "Leakage Volume (mL)",
        "LeakageLocation": "Leakage Location",
        "ElectrolyteStirringPerformed": "Electrolyte Stirring Performed",
        "ReservoirStirringMethod": "Reservoir Stirring Method",
        "StirringRate": "Stirring Rate (RPM)",
        "ElectrolyteSparging": "Electrolyte Sparging Performed",
        "SpargingGas": "Sparging Gas",
        "GasHumidification": "Gas Humidification",
        "SpargingApplication": "Sparging Application",
        "SpargingGasFlowRate": "Sparging Gas Flow Rate",
        "PolarisationBeyond60mAcm2": "Polarisation Beyond ±60 mA/cm²",
        "MaxCurrentDensity": "Maximum Current Density Used (mA/cm²)",
        "RationaleForMaxCurrentDensity": "Rationale for Maximum Current Density",
        "ImpedanceControlMode": "Impedance Control Mode",
        "RationaleForImpedanceTechnique": "Rationale for Impedance Technique",
        "BasisForImpedanceParameters": "Basis for Selecting Impedance Parameters",
        "ReservoirTubingChanges": "Reservoir/Tubing Changes Observed During Cycling",
        "ObservableChangesAfterDisassembly": "Observable Changes After Cell Disassembly",
        "ComponentsAffected": "Components Affected",
        "NatureOfDiscolourationOrDeposits": "Nature of Discolouration or Deposits",
        "AdditionalTestsPerformed": "Additional Tests Performed",
        "AdditionalProtocolDetails": "Additional Protocol or Setup Details",
    }

    _sections = {
        "Participant Details": ["Phase", "JobRole", "Experience", "ParticipantID", "NumRepeats"],
        "Electrode Treatment & Handling": ["ElectrodePreTreatmentType", "HeatTreatmentFurnaceType", "HeatTreatmentFurnaceDetails", "HeatTreatmentAtmosphere", "HeatTreatmentRampRate", "HeatTreatmentDwellTemp", "HeatTreatmentDwellTime", "HeatTreatmentCoolingMethod", "HeatTreatmentBatching", "TimeBetweenHeatTreatmentAndUse", "ElectrodeStorageAfterHeatTreatment", "HeatTreatmentCoolingDetails", "ElectrodeStorageAfterReceipt", "ElectrodeCuttingMethod"],
        "Membrane Handling": ["MembraneStorageType", "LiquidForMembraneStorage", "MembraneStorageDetails", "MembranePreTreatment", "NumMembranePreTreatmentSteps", "MembranePreTreatmentApplied", "MembranePreTreatmentMedium", "MembranePreTreatmentMediumConc", "MembranePreTreatmentDuration", "MembranePreTreatmentTemp", "AdditionalMembranePreTreatmentDetails"],
        "Electrolyte Preparation": ["PotassiumFerrocyanideDetails", "PotassiumFerrocyanidePurity", "PotassiumFerrocyanideSupplier", "PotassiumFerricyanideIdentity", "PotassiumFerricyanidePurity", "PotassiumFerricyanideSupplier", "SupportingElectrolyteIdentity", "PotassiumChloridePurity", "PotassiumChlorideSupplier", "WaterType", "WaterPurificationSystem", "WaterPurificationSystemDetails", "WaterResistivity", "ElectrolytePreparationStrategy", "ElectrolyteStockSize", "ElectrolyteStorageEnvironment", "ElectrolyteStorageDuration", "ElectrolyteVolumeMeasurementMethod", "VolumeMeasurementInstrumentType", "ReagentPurityUsedInCalculations", "ReagentPurityUsageDetails", "WaterPHMeasured", "WaterPHDetails", "ElectrolytePHMeasured", "ElectrolytePHValue"],
        "Potentiostat Usage": ["PotentiostatUsage", "PotentiostatManufacturer", "PotentiostatModel", "PotentiostatManufacturerCycling", "PotentiostatModelCycling", "PotentiostatManufacturerPolarisation", "PotentiostatModelPolarisation", "PotentiostatManufacturerImpedance", "PotentiostatModelImpedance", "PotentiostatCalibrationFrequency", "PotentiostatCalibrationMethod", "ConnectionMethodToCell", "ElectricalConnectionScheme", "PotentiostatCableLength", "PotentiostatCableType", "PotentiostatCableShielding"],
        "Fluidics (Pumps, Tanks & Tubing)": ["NumPumps", "PumpType", "PumpManufacturer", "PumpModel", "ReservoirMaterial", "ReservoirType", "ReservoirVolume", "TubingMaterial", "TubingManufacturer", "TubingInnerDiameter", "WettedFittingMaterials", "OtherWettedMaterials"],
        "Test Protocols": ["TemperatureControl", "TestTemperature", "ElectrolyteVolumeUsed", "ImpedanceReservoirConfig", "PolarisationReservoirConfig", "PumpFlowRateCalibrated", "CalibrationPerformedWithCellInLine", "CalibrationFluidUsed", "PumpRecalibratedDuringExperiments", "PumpCalibrationMethod", "LeakTestPerformed", "LeakTestDuration", "LeakTestFlowRate", "PreConditioningPerformed", "BreakInPeriodDuration", "BreakInPeriodFlowRate", "CellAssemblyIssues", "AdditionalExperimentsPerformed", "AdditionalExperimentRationale", "RepeatsTechniquesRepeated", "RepeatMethod", "TypicalGroupPracticeForLeakingCells", "ElectrolyteLeakage", "LeakageDescription", "LeakageSeverity", "LeakageVolume", "LeakageLocation", "ElectrolyteStirringPerformed", "ReservoirStirringMethod", "StirringRate", "ElectrolyteSparging", "SpargingGas", "GasHumidification", "SpargingApplication", "SpargingGasFlowRate", "PolarisationBeyond60mAcm2", "MaxCurrentDensity", "RationaleForMaxCurrentDensity"],
        "Observations": ["ReservoirTubingChanges", "ObservableChangesAfterDisassembly", "ComponentsAffected", "NatureOfDiscolourationOrDeposits"],
        "Additional Information": ["AdditionalTestsPerformed", "AdditionalProtocolDetails"],
    }

    _row_height = 37
    _max_rows = 5
    _max_height = _row_height * _max_rows

    _df_flowrates_2a = (
        df_pol
        .filter(pl.col("Phase") == "2a")
        .select(["ParticipantID", "FlowRate"])
        .unique()
        .sort(["ParticipantID", "FlowRate"])
        .group_by("ParticipantID")
        .agg(pl.col("FlowRate").sort().cast(pl.Utf8).str.join(", ").alias("FlowRates"))
    )

    _cell_header_cols = ["Participant ID", "Cell", "Flow Field", "Area (cm²)", "Membrane", "Electrode", "Tank Volume (mL)", "Flow Rate(s) (mL/min)", "Polarisation Beyond ±60 mA/cm²", "Max Current Density (mA/cm²)", "Rationale for Max Current Density"]
    _cell_header_html = "".join(
        f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
        for h in _cell_header_cols
    )

    _cell_rows_html = ""
    for _pid, _phase in _all_selected:
        _color = _pid_color_map[(_pid, _phase)]
        _pid_cell = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
        </td>"""
        if _phase == "2a":
            _meta_row = df_meta_2a.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                continue
            _m = _meta_row.to_dicts()[0]
            _fr_row = _df_flowrates_2a.filter(pl.col("ParticipantID") == _pid)
            _fr = _fr_row["FlowRates"][0] if len(_fr_row) > 0 else "—"
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Cell", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("FF", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Area_cm2", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Membrane", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Electrode", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_fr}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
            </tr>"""
        else:
            _meta_row = _df_meta_2b_display.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                _cell_rows_html += f"""<tr>{_pid_cell}
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(_cell_header_cols) - 1}">Metadata not yet submitted for 2b</td>
                </tr>"""
                continue
            _m = _meta_row.to_dicts()[0]
            _tank = _m.get("ElectrolyteVolumeUsed", "—")
            _pol_beyond = _m.get("PolarisationBeyond60mAcm2", "—")
            _max_cd = _m.get("MaxCurrentDensity", "—")
            _rationale = _m.get("RationaleForMaxCurrentDensity", "—")
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">QUB</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">FTFF</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">16</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">N117</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">SGL GFD 4.65 EA</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_tank if _tank else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">25</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_pol_beyond if _pol_beyond else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_max_cd if _max_cd else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_rationale if _rationale else "—"}</td>
            </tr>"""

    _cell_table_html = f"""
    <div style="margin-bottom:16px;">
        <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">Cell and Test Details</div>
        <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
            <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                <thead><tr>{_cell_header_html}</tr></thead>
                <tbody>{_cell_rows_html}</tbody>
            </table>
        </div>
    </div>
    """ if _cell_rows_html else ""

    def _build_section_table(section_title, cols, all_selected, pid_color_map, df_2b):
        _header_cols = ["Participant ID"] + [_col_display_names.get(c, c) for c in cols]
        _header_html = "".join(
            f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
            for h in _header_cols
        )
        _rows_html = ""
        for _pid, _phase in all_selected:
            _color = pid_color_map[(_pid, _phase)]
            if _phase == "2a":
                _rows_html += f"""
                <tr>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                        <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                    </td>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Phase 2a metadata not yet formatted</td>
                </tr>
                """
            else:
                _row = df_2b.filter(pl.col("ParticipantID") == _pid)
                if len(_row) == 0:
                    _rows_html += f"""
                    <tr>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                        </td>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Metadata not yet submitted for 2b</td>
                    </tr>
                    """
                    continue
                _row_dict = _row.to_dicts()[0]
                _cells = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                    <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                </td>"""
                for _c in cols:
                    _v = _row_dict.get(_c, "")
                    _cells += f'<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap; max-width:300px; overflow:hidden; text-overflow:ellipsis;">{_v if _v else "—"}</td>'
                _rows_html += f"<tr>{_cells}</tr>"

        if not _rows_html:
            return ""

        return f"""
        <div style="margin-bottom:16px;">
            <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">{section_title}</div>
            <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
                <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                    <thead><tr>{_header_html}</tr></thead>
                    <tbody>{_rows_html}</tbody>
                </table>
            </div>
        </div>
        """

    _section_items = list(_sections.items())

    _all_section_htmls = []
    for _title, _cols in _section_items:
        _all_section_htmls.append(_build_section_table(_title, _cols, _all_selected, _pid_color_map, _df_meta_2b_display))

    _grid_cells = f"""
    <div style="min-width:0;">{_cell_table_html}</div>
    <div style="min-width:0;">{_all_section_htmls[0]}</div>
    """
    for _i in range(1, len(_all_section_htmls), 2):
        _left = _all_section_htmls[_i]
        _right = _all_section_htmls[_i + 1] if _i + 1 < len(_all_section_htmls) else ""
        _grid_cells += f"""
        <div style="min-width:0;">{_left}</div>
        <div style="min-width:0;">{_right}</div>
        """

    mo.stop(_grid_cells.strip() == "", mo.Html('<div style="margin-top:16px; color:#888; font-style:italic;">No metadata available for selected participants.</div>'))

    mo.Html(f"""
    <div style="margin-top:16px;">
        <div style="font-size:15px; font-weight:700; margin-bottom:16px;">Selected Participant Metadata</div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:24px; overflow:visible;">
            {_grid_cells}
        </div>
    </div>
    """)
    return


@app.cell
def _(mo):
    mo.Html("""
    <div style="border-top: 4px solid #000; margin: 40px 0 24px 0; padding-top: 16px;">
      <h2 style="color: #000; margin:0; font-size: 28px; font-weight: 700;">📡 EIS</h2>
      <p style="color: #000; margin: 4px 0 0 0;">Electrochemical impedance spectroscopy data across all participants and repeats.</p>
    </div>
    """)
    return


@app.cell
def _(df_metrics, mo, pl):
    eis_participant_selector_2a = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2a")["ParticipantID"].unique().to_list()),
        label="Phase 2a — Participants"
    )
    eis_repeat_selector_2a = mo.ui.multiselect(
        options=[1, 2, 3],
        label="Phase 2a — Repeats"
    )
    eis_participant_selector_2b = mo.ui.multiselect(
        options=sorted(df_metrics.filter(pl.col("Phase") == "2b")["ParticipantID"].unique().to_list()),
        label="Phase 2b — Participants"
    )
    eis_repeat_selector_2b = mo.ui.multiselect(
        options=[1, 2, 3],
        label="Phase 2b — Repeats"
    )

    mo.Html(f"""
    <div style="display:flex; justify-content:center; gap:90px; margin: 8px 0;">
        <div style="display:flex; flex-direction:column; gap:30px; width:280px;">
            {mo.as_html(eis_participant_selector_2a).text}
            {mo.as_html(eis_repeat_selector_2a).text}
        </div>
        <div style="display:flex; flex-direction:column; gap:30px; width:280px;">
            {mo.as_html(eis_participant_selector_2b).text}
            {mo.as_html(eis_repeat_selector_2b).text}
        </div>
    </div>
    """)
    return (
        eis_participant_selector_2a,
        eis_participant_selector_2b,
        eis_repeat_selector_2a,
        eis_repeat_selector_2b,
    )


@app.cell
def _(
    client,
    eis_participant_selector_2a,
    eis_participant_selector_2b,
    mo,
    pl,
):
    _eis_flowrates_available = pl.from_arrow(
        client.query(f"""
            SELECT DISTINCT FlowRate
            FROM `flow-battery-data-ingestion.electrochem.eis-data`
            WHERE Phase = '2a'
            {"AND ParticipantID IN (" + ", ".join(f"'{p}'" for p in eis_participant_selector_2a.value) + ")" if eis_participant_selector_2a.value else ""}
            ORDER BY FlowRate
        """).to_arrow()
    )["FlowRate"].to_list()

    eis_flowrate_selector = mo.ui.multiselect(
        options=_eis_flowrates_available,
        label="Phase 2a — Flow Rates"
    )

    mo.Html(f"""
    <div style="display:flex; justify-content:center; gap:65px; margin: 8px 0;">
        <div style="display:flex; flex-direction:column; gap:20px; width:280px;">
            {mo.as_html(eis_flowrate_selector).text}
        </div>
        <div style="visibility:hidden; display:flex; flex-direction:column; gap:20px; width:310px;">
            {mo.as_html(eis_participant_selector_2b).text}
        </div>
    </div>
    """)
    return (eis_flowrate_selector,)


@app.cell
def _(
    alt,
    client,
    eis_flowrate_selector,
    eis_participant_selector_2a,
    eis_participant_selector_2b,
    eis_repeat_selector_2a,
    eis_repeat_selector_2b,
    mo,
    pl,
):
    _eis_participants_2a = eis_participant_selector_2a.value
    _eis_participants_2b = eis_participant_selector_2b.value
    _eis_repeats_2a = eis_repeat_selector_2a.value
    _eis_repeats_2b = eis_repeat_selector_2b.value
    _eis_flowrates = eis_flowrate_selector.value
    if not _eis_participants_2a and not _eis_participants_2b:
        mo.stop(True, mo.center(mo.callout(mo.md("**Please select at least one participant to load EIS data.**"), kind="warn")))
    _flowrate_filter = f"AND FlowRate IN ({', '.join(str(f) for f in _eis_flowrates)})" if _eis_flowrates else ""
    def _query_eis(phase, participants, repeats):
        _plist = ", ".join(f"'{p}'" for p in participants)
        _repeat_filter = f"AND Repeat IN ({', '.join(str(r) for r in repeats)})" if repeats else ""
        _fr_filter = _flowrate_filter if phase == "2a" else ""
        if phase == "2b":
            _zr_col = "Zi_ohm"
            _zi_col = "Zr_ohm"
        else:
            _zr_col = "Zr_ohm"
            _zi_col = "Zi_ohm"
        _df = pl.from_arrow(
            client.query(f"""
                SELECT ParticipantID, FlowRate, Repeat, Frequency_Hz,
                       {_zr_col} AS Zr_ohm, {_zi_col} AS Zi_ohm
                FROM `flow-battery-data-ingestion.electrochem.eis-data`
                WHERE Phase = '{phase}'
                AND ParticipantID IN ({_plist})
                {_repeat_filter}
                {_fr_filter}
                AND Frequency_Hz <= 10000
                ORDER BY ParticipantID, Repeat, FlowRate, Frequency_Hz DESC
            """).to_arrow()
        )
        _neg = 1.0 if phase == "2b" else -1.0
        return _df.with_columns([
            (pl.col("Zi_ohm") * _neg).alias("neg_Zi_ohm"),
            (pl.col("ParticipantID") + " — " + pl.lit(phase)).alias("ParticipantKey"),
            pl.col("Repeat").cast(pl.Utf8).alias("RepeatStr"),
            pl.col("FlowRate").cast(pl.Utf8).alias("FlowRateStr"),
        ])
    _frames = []
    if _eis_participants_2a:
        _frames.append(_query_eis("2a", _eis_participants_2a, _eis_repeats_2a))
    if _eis_participants_2b:
        _frames.append(_query_eis("2b", _eis_participants_2b, _eis_repeats_2b))
    _df_eis = pl.concat(_frames).to_pandas()

    _selected = [("2a", p) for p in _eis_participants_2a] + [("2b", p) for p in _eis_participants_2b]
    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _color_map = {f"{p} — {phase}": _palette[i % len(_palette)] for i, (phase, p) in enumerate(_selected)}
    _color_domain = list(_color_map.keys())
    _color_range = list(_color_map.values())

    _re_min = _df_eis["Zr_ohm"].min()
    _re_max = _df_eis["Zr_ohm"].max()
    _im_min = _df_eis["neg_Zi_ohm"].min()
    _im_max = _df_eis["neg_Zi_ohm"].max()
    _re_range = _re_max - _re_min
    _im_range = _im_max - _im_min
    _max_range = max(_re_range, _im_range)
    _padding = _max_range * 0.05
    _re_mid = (_re_min + _re_max) / 2
    _im_mid = (_im_min + _im_max) / 2
    _half = (_max_range / 2) + _padding
    _re_domain = [_re_mid - _half, _re_mid + _half]
    _im_domain = [_im_mid - _half, _im_mid + _half]

    _participant_sel = alt.selection_point(fields=["ParticipantKey"], bind="legend")
    _repeat_sel = alt.selection_point(fields=["RepeatStr"], bind="legend")
    _flowrate_sel = alt.selection_point(fields=["FlowRateStr"], bind="legend")
    alt.data_transformers.enable("default", max_rows=None)
    alt.renderers.set_embed_options(renderer="canvas")
    _eis_chart = alt.Chart(_df_eis).mark_point().encode(
        x=alt.X("Zr_ohm:Q", title="Re(Z) / Ω", scale=alt.Scale(domain=_re_domain)),
        y=alt.Y("neg_Zi_ohm:Q", title="-Im(Z) / Ω", scale=alt.Scale(domain=_im_domain)),
        color=alt.Color("ParticipantKey:N",
            scale=alt.Scale(domain=_color_domain, range=_color_range),
            title="Participant"),
        shape=alt.Shape("RepeatStr:N", title="Repeat",
            scale=alt.Scale(range=["circle", "square", "triangle-up"])),
        size=alt.Size("FlowRateStr:N", title="Flow Rate (mL min⁻¹)",
            scale=alt.Scale(range=[30, 150])),
        opacity=alt.condition(
            _participant_sel & _repeat_sel & _flowrate_sel,
            alt.value(1.0),
            alt.value(0.025)
        ),
        tooltip=["ParticipantKey", "RepeatStr", "FlowRateStr", "Frequency_Hz", "Zr_ohm", "Zi_ohm"]
    ).properties(
        width=500,
        height=500,
    ).add_params(
        _participant_sel,
        _repeat_sel,
        _flowrate_sel,
        alt.selection_interval(bind="scales", zoom="wheel![event.shiftKey]")
    )
    mo.Html(f'<div style="overflow-x:auto; text-align:center;">{mo.as_html(_eis_chart).text}</div>')
    return


@app.cell
def _(mo):
    mo.center(mo.Html("<p style='font-size:12px; color:#888;'>💡 Hold <b>Shift + scroll</b> to zoom · <b>Click</b> and <b>drag</b> to pan · <b>Double-click</b> to reset</p>"))
    return


@app.cell
def _(
    df_meta_2a,
    df_meta_2b,
    df_pol,
    eis_participant_selector_2a,
    eis_participant_selector_2b,
    mo,
    pl,
):
    _selected_2a = eis_participant_selector_2a.value
    _selected_2b = eis_participant_selector_2b.value
    _all_selected = [(p, "2a") for p in _selected_2a] + [(p, "2b") for p in _selected_2b]

    mo.stop(len(_all_selected) == 0)

    _palette = [
        "#6366F1", "#EC4899", "#14B8A6", "#F59E0B", "#3B82F6",
        "#10B981", "#F97316", "#8B5CF6", "#EF4444", "#06B6D4",
        "#84CC16", "#F43F5E", "#0EA5E9", "#A855F7", "#22C55E",
    ]
    _pid_color_map = {(p, phase): _palette[i % len(_palette)] for i, (p, phase) in enumerate(_all_selected)}

    _df_meta_2b_display = df_meta_2b.filter(
        pl.col("ConsentToPublicDisplay").str.to_lowercase().str.contains("yes")
    ).drop("ConsentToPublicDisplay")

    _col_display_names = {
        "Phase": "Phase",
        "JobRole": "Job Role",
        "Experience": "Experience",
        "ParticipantID": "Participant ID",
        "NumRepeats": "Number of Repeats",
        "ElectrodePreTreatmentType": "Electrode Pre-Treatment Type",
        "HeatTreatmentFurnaceType": "Heat Treatment Furnace Type",
        "HeatTreatmentFurnaceDetails": "Heat Treatment Furnace Details",
        "HeatTreatmentAtmosphere": "Heat Treatment Atmosphere",
        "HeatTreatmentRampRate": "Heat Treatment Ramp Rate (°C/min)",
        "HeatTreatmentDwellTemp": "Heat Treatment Dwell Temperature (°C)",
        "HeatTreatmentDwellTime": "Heat Treatment Dwell Time (h)",
        "HeatTreatmentCoolingMethod": "Heat Treatment Cooling Method",
        "HeatTreatmentBatching": "Heat Treatment Batching",
        "TimeBetweenHeatTreatmentAndUse": "Time Between Heat Treatment and Use (h)",
        "ElectrodeStorageAfterHeatTreatment": "Electrode Storage After Heat Treatment",
        "HeatTreatmentCoolingDetails": "Heat Treatment Cooling Details",
        "ElectrodeStorageAfterReceipt": "Electrode Storage After Receipt",
        "ElectrodeCuttingMethod": "Electrode Cutting and Sizing Method",
        "MembraneStorageType": "Membrane Storage Type",
        "LiquidForMembraneStorage": "Liquid for Membrane Storage",
        "MembraneStorageDetails": "Membrane Storage Details",
        "MembranePreTreatment": "Membrane Pre-Treatment",
        "NumMembranePreTreatmentSteps": "Number of Membrane Pre-Treatment Steps",
        "MembranePreTreatmentApplied": "Membrane Pre-Treatment Applied",
        "MembranePreTreatmentMedium": "Membrane Pre-Treatment Medium",
        "MembranePreTreatmentMediumConc": "Membrane Pre-Treatment Medium Concentration (M)",
        "MembranePreTreatmentDuration": "Membrane Pre-Treatment Duration (h)",
        "MembranePreTreatmentTemp": "Membrane Pre-Treatment Temperature (°C)",
        "AdditionalMembranePreTreatmentDetails": "Additional Membrane Pre-Treatment Details",
        "PotassiumFerrocyanideDetails": "Potassium Ferrocyanide Chemical Details",
        "PotassiumFerrocyanidePurity": "Potassium Ferrocyanide Purity (%)",
        "PotassiumFerrocyanideSupplier": "Potassium Ferrocyanide Supplier",
        "PotassiumFerricyanideIdentity": "Potassium Ferricyanide Chemical Identity",
        "PotassiumFerricyanidePurity": "Potassium Ferricyanide Purity (%)",
        "PotassiumFerricyanideSupplier": "Potassium Ferricyanide Supplier",
        "SupportingElectrolyteIdentity": "Supporting Electrolyte Chemical Identity",
        "PotassiumChloridePurity": "Potassium Chloride Purity (%)",
        "PotassiumChlorideSupplier": "Potassium Chloride Supplier",
        "WaterType": "Water Type",
        "WaterPurificationSystem": "Water Purification System",
        "WaterPurificationSystemDetails": "Water Purification System Details",
        "WaterResistivity": "Water Resistivity (MΩ·cm)",
        "ElectrolytePreparationStrategy": "Electrolyte Preparation Strategy",
        "ElectrolyteStockSize": "Electrolyte Stock/Batch Size (mL)",
        "ElectrolyteStorageEnvironment": "Electrolyte Storage Environment",
        "ElectrolyteStorageDuration": "Electrolyte Storage Duration (h)",
        "ElectrolyteVolumeMeasurementMethod": "Electrolyte Volume Measurement Method",
        "VolumeMeasurementInstrumentType": "Volume Measurement Instrument Type",
        "ReagentPurityUsedInCalculations": "Reagent Purity Used in Calculations",
        "ReagentPurityUsageDetails": "Reagent Purity Usage Details",
        "WaterPHMeasured": "Water pH Measured",
        "WaterPHDetails": "Water pH Details",
        "ElectrolytePHMeasured": "Electrolyte pH Measured",
        "ElectrolytePHValue": "Electrolyte pH Value",
        "PotentiostatUsage": "Potentiostat Usage",
        "PotentiostatManufacturer": "Potentiostat Manufacturer",
        "PotentiostatModel": "Potentiostat Model",
        "PotentiostatManufacturerCycling": "Potentiostat Manufacturer (Cycling)",
        "PotentiostatModelCycling": "Potentiostat Model (Cycling)",
        "PotentiostatManufacturerPolarisation": "Potentiostat Manufacturer (Polarisation)",
        "PotentiostatModelPolarisation": "Potentiostat Model (Polarisation)",
        "PotentiostatManufacturerImpedance": "Potentiostat Manufacturer (Impedance)",
        "PotentiostatModelImpedance": "Potentiostat Model (Impedance)",
        "PotentiostatCalibrationFrequency": "Potentiostat Calibration Frequency",
        "PotentiostatCalibrationMethod": "Potentiostat Calibration Method",
        "ConnectionMethodToCell": "Connection Method to Cell",
        "ElectricalConnectionScheme": "Electrical Connection Scheme",
        "PotentiostatCableLength": "Potentiostat Cable Length",
        "PotentiostatCableType": "Potentiostat Cable Type",
        "PotentiostatCableShielding": "Potentiostat Cable Shielding",
        "NumPumps": "Number of Pumps",
        "PumpType": "Pump Type",
        "PumpManufacturer": "Pump Manufacturer",
        "PumpModel": "Pump Model",
        "ReservoirMaterial": "Reservoir Material",
        "ReservoirType": "Reservoir Type",
        "ReservoirVolume": "Reservoir Volume (mL)",
        "TubingMaterial": "Tubing Material",
        "TubingManufacturer": "Tubing Manufacturer",
        "TubingInnerDiameter": "Tubing Inner Diameter",
        "WettedFittingMaterials": "Wetted Fitting Materials",
        "OtherWettedMaterials": "Other Wetted Materials",
        "ElectrolyteVolumeUsed": "Electrolyte Volume Used (mL)",
        "ImpedanceReservoirConfig": "Impedance Reservoir Configuration",
        "PolarisationReservoirConfig": "Polarisation Reservoir Configuration",
        "TemperatureControl": "Temperature Control",
        "TestTemperature": "Test Temperature",
        "PumpFlowRateCalibrated": "Pump Flow Rate Calibrated",
        "CalibrationPerformedWithCellInLine": "Calibration Performed With Cell In Line",
        "CalibrationFluidUsed": "Calibration Fluid Used",
        "PumpRecalibratedDuringExperiments": "Pump Recalibrated During Experiments",
        "PumpCalibrationMethod": "Pump Calibration Method",
        "LeakTestPerformed": "Leak Test Performed",
        "LeakTestDuration": "Leak Test Duration (h)",
        "LeakTestFlowRate": "Leak Test Flow Rate (mL/min)",
        "PreConditioningPerformed": "Pre-Conditioning/Break-in Period Performed",
        "BreakInPeriodDuration": "Break-in Period Duration (h)",
        "BreakInPeriodFlowRate": "Break-in Period Flow Rate (mL/min)",
        "CellAssemblyIssues": "Cell Assembly Issues",
        "AdditionalExperimentsPerformed": "Additional Experiments Performed",
        "AdditionalExperimentRationale": "Additional Experiment Rationale",
        "RepeatsTechniquesRepeated": "Repeat(s) and Technique(s) Repeated",
        "RepeatMethod": "Repeat Method",
        "TypicalGroupPracticeForLeakingCells": "Typical Group Practice for Leaking Cells",
        "ElectrolyteLeakage": "Electrolyte Leakage (Reported Data Only)",
        "LeakageDescription": "Leakage Description (Reported Data Only)",
        "LeakageSeverity": "Leakage Severity",
        "LeakageVolume": "Leakage Volume (mL)",
        "LeakageLocation": "Leakage Location",
        "ElectrolyteStirringPerformed": "Electrolyte Stirring Performed",
        "ReservoirStirringMethod": "Reservoir Stirring Method",
        "StirringRate": "Stirring Rate (RPM)",
        "ElectrolyteSparging": "Electrolyte Sparging Performed",
        "SpargingGas": "Sparging Gas",
        "GasHumidification": "Gas Humidification",
        "SpargingApplication": "Sparging Application",
        "SpargingGasFlowRate": "Sparging Gas Flow Rate",
        "ImpedanceControlMode": "Impedance Control Mode",
        "RationaleForImpedanceTechnique": "Rationale for Impedance Technique",
        "BasisForImpedanceParameters": "Basis for Selecting Impedance Parameters",
        "ReservoirTubingChanges": "Reservoir/Tubing Changes Observed During Cycling",
        "ObservableChangesAfterDisassembly": "Observable Changes After Cell Disassembly",
        "ComponentsAffected": "Components Affected",
        "NatureOfDiscolourationOrDeposits": "Nature of Discolouration or Deposits",
        "AdditionalTestsPerformed": "Additional Tests Performed",
        "AdditionalProtocolDetails": "Additional Protocol or Setup Details",
    }

    _sections = {
        "Participant Details": ["Phase", "JobRole", "Experience", "ParticipantID", "NumRepeats"],
        "Electrode Treatment & Handling": ["ElectrodePreTreatmentType", "HeatTreatmentFurnaceType", "HeatTreatmentFurnaceDetails", "HeatTreatmentAtmosphere", "HeatTreatmentRampRate", "HeatTreatmentDwellTemp", "HeatTreatmentDwellTime", "HeatTreatmentCoolingMethod", "HeatTreatmentBatching", "TimeBetweenHeatTreatmentAndUse", "ElectrodeStorageAfterHeatTreatment", "HeatTreatmentCoolingDetails", "ElectrodeStorageAfterReceipt", "ElectrodeCuttingMethod"],
        "Membrane Handling": ["MembraneStorageType", "LiquidForMembraneStorage", "MembraneStorageDetails", "MembranePreTreatment", "NumMembranePreTreatmentSteps", "MembranePreTreatmentApplied", "MembranePreTreatmentMedium", "MembranePreTreatmentMediumConc", "MembranePreTreatmentDuration", "MembranePreTreatmentTemp", "AdditionalMembranePreTreatmentDetails"],
        "Electrolyte Preparation": ["PotassiumFerrocyanideDetails", "PotassiumFerrocyanidePurity", "PotassiumFerrocyanideSupplier", "PotassiumFerricyanideIdentity", "PotassiumFerricyanidePurity", "PotassiumFerricyanideSupplier", "SupportingElectrolyteIdentity", "PotassiumChloridePurity", "PotassiumChlorideSupplier", "WaterType", "WaterPurificationSystem", "WaterPurificationSystemDetails", "WaterResistivity", "ElectrolytePreparationStrategy", "ElectrolyteStockSize", "ElectrolyteStorageEnvironment", "ElectrolyteStorageDuration", "ElectrolyteVolumeMeasurementMethod", "VolumeMeasurementInstrumentType", "ReagentPurityUsedInCalculations", "ReagentPurityUsageDetails", "WaterPHMeasured", "WaterPHDetails", "ElectrolytePHMeasured", "ElectrolytePHValue"],
        "Potentiostat Usage": ["PotentiostatUsage", "PotentiostatManufacturer", "PotentiostatModel", "PotentiostatManufacturerCycling", "PotentiostatModelCycling", "PotentiostatManufacturerPolarisation", "PotentiostatModelPolarisation", "PotentiostatManufacturerImpedance", "PotentiostatModelImpedance", "PotentiostatCalibrationFrequency", "PotentiostatCalibrationMethod", "ConnectionMethodToCell", "ElectricalConnectionScheme", "PotentiostatCableLength", "PotentiostatCableType", "PotentiostatCableShielding"],
        "Fluidics (Pumps, Tanks & Tubing)": ["NumPumps", "PumpType", "PumpManufacturer", "PumpModel", "ReservoirMaterial", "ReservoirType", "ReservoirVolume", "TubingMaterial", "TubingManufacturer", "TubingInnerDiameter", "WettedFittingMaterials", "OtherWettedMaterials"],
        "Test Protocols": ["TemperatureControl", "TestTemperature", "ElectrolyteVolumeUsed", "ImpedanceReservoirConfig", "PolarisationReservoirConfig", "PumpFlowRateCalibrated", "CalibrationPerformedWithCellInLine", "CalibrationFluidUsed", "PumpRecalibratedDuringExperiments", "PumpCalibrationMethod", "LeakTestPerformed", "LeakTestDuration", "LeakTestFlowRate", "PreConditioningPerformed", "BreakInPeriodDuration", "BreakInPeriodFlowRate", "CellAssemblyIssues", "AdditionalExperimentsPerformed", "AdditionalExperimentRationale", "RepeatsTechniquesRepeated", "RepeatMethod", "TypicalGroupPracticeForLeakingCells", "ElectrolyteLeakage", "LeakageDescription", "LeakageSeverity", "LeakageVolume", "LeakageLocation", "ElectrolyteStirringPerformed", "ReservoirStirringMethod", "StirringRate", "ElectrolyteSparging", "SpargingGas", "GasHumidification", "SpargingApplication", "SpargingGasFlowRate", "ImpedanceControlMode", "RationaleForImpedanceTechnique", "BasisForImpedanceParameters"],
        "Observations": ["ReservoirTubingChanges", "ObservableChangesAfterDisassembly", "ComponentsAffected", "NatureOfDiscolourationOrDeposits"],
        "Additional Information": ["AdditionalTestsPerformed", "AdditionalProtocolDetails"],
    }

    _row_height = 37
    _max_rows = 5
    _max_height = _row_height * _max_rows

    _df_flowrates_2a = (
        df_pol
        .filter(pl.col("Phase") == "2a")
        .select(["ParticipantID", "FlowRate"])
        .unique()
        .sort(["ParticipantID", "FlowRate"])
        .group_by("ParticipantID")
        .agg(pl.col("FlowRate").sort().cast(pl.Utf8).str.join(", ").alias("FlowRates"))
    )

    _cell_header_cols = ["Participant ID", "Cell", "Flow Field", "Area (cm²)", "Membrane", "Electrode", "Tank Volume (mL)", "Flow Rate(s) (mL/min)", "Impedance Control Mode", "Rationale for Impedance Technique", "Basis for Impedance Parameters"]
    _cell_header_html = "".join(
        f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
        for h in _cell_header_cols
    )

    _cell_rows_html = ""
    for _pid, _phase in _all_selected:
        _color = _pid_color_map[(_pid, _phase)]
        _pid_cell = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
        </td>"""
        if _phase == "2a":
            _meta_row = df_meta_2a.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                continue
            _m = _meta_row.to_dicts()[0]
            _fr_row = _df_flowrates_2a.filter(pl.col("ParticipantID") == _pid)
            _fr = _fr_row["FlowRates"][0] if len(_fr_row) > 0 else "—"
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Cell", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("FF", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Area_cm2", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Membrane", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_m.get("Electrode", "—")}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_fr}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;">Phase 2a metadata not yet formatted</td>
            </tr>"""
        else:
            _meta_row = _df_meta_2b_display.filter(pl.col("ParticipantID") == _pid)
            if len(_meta_row) == 0:
                _cell_rows_html += f"""<tr>{_pid_cell}
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(_cell_header_cols) - 1}">Metadata not yet submitted for 2b</td>
                </tr>"""
                continue
            _m = _meta_row.to_dicts()[0]
            _tank = _m.get("ElectrolyteVolumeUsed", "—")
            _imp_mode = _m.get("ImpedanceControlMode", "—")
            _imp_rationale = _m.get("RationaleForImpedanceTechnique", "—")
            _imp_basis = _m.get("BasisForImpedanceParameters", "—")
            _cell_rows_html += f"""<tr>{_pid_cell}
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">QUB</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">FTFF</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">16</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">N117</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">SGL GFD 4.65 EA</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_tank if _tank else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">25</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_imp_mode if _imp_mode else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_imp_rationale if _imp_rationale else "—"}</td>
                <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap;">{_imp_basis if _imp_basis else "—"}</td>
            </tr>"""

    _cell_table_html = f"""
    <div style="margin-bottom:16px;">
        <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">Cell and Test Details</div>
        <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
            <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                <thead><tr>{_cell_header_html}</tr></thead>
                <tbody>{_cell_rows_html}</tbody>
            </table>
        </div>
    </div>
    """ if _cell_rows_html else ""

    def _build_section_table(section_title, cols, all_selected, pid_color_map, df_2b):
        _header_cols = ["Participant ID"] + [_col_display_names.get(c, c) for c in cols]
        _header_html = "".join(
            f'<th style="padding:8px 12px; text-align:left; border-bottom:2px solid #e5e7eb; white-space:nowrap; background:#f8f8f8; position:sticky; top:0; z-index:1;">{h}</th>'
            for h in _header_cols
        )
        _rows_html = ""
        for _pid, _phase in all_selected:
            _color = pid_color_map[(_pid, _phase)]
            if _phase == "2a":
                _rows_html += f"""
                <tr>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                        <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                    </td>
                    <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Phase 2a metadata not yet formatted</td>
                </tr>
                """
            else:
                _row = df_2b.filter(pl.col("ParticipantID") == _pid)
                if len(_row) == 0:
                    _rows_html += f"""
                    <tr>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                            <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                        </td>
                        <td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; color:#888; font-style:italic; white-space:nowrap;" colspan="{len(cols)}">Metadata not yet submitted for 2b</td>
                    </tr>
                    """
                    continue
                _row_dict = _row.to_dicts()[0]
                _cells = f"""<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; font-weight:600; white-space:nowrap;">
                    <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{_color}; margin-right:6px;"></span>{_pid}
                </td>"""
                for _c in cols:
                    _v = _row_dict.get(_c, "")
                    _cells += f'<td style="padding:8px 12px; border-bottom:1px solid #f0f0f0; white-space:nowrap; max-width:300px; overflow:hidden; text-overflow:ellipsis;">{_v if _v else "—"}</td>'
                _rows_html += f"<tr>{_cells}</tr>"

        if not _rows_html:
            return ""

        return f"""
        <div style="margin-bottom:16px;">
            <div style="font-size:13px; font-weight:700; color:#374151; margin-bottom:6px; padding:4px 0; border-bottom:2px solid #6366F1;">{section_title}</div>
            <div style="display:block; width:100%; overflow-x:auto; overflow-y:auto; max-height:{_max_height}px; border:1px solid #e5e7eb; border-radius:8px;">
                <table style="border-collapse:collapse; font-size:12px; min-width:100%;">
                    <thead><tr>{_header_html}</tr></thead>
                    <tbody>{_rows_html}</tbody>
                </table>
            </div>
        </div>
        """

    _section_items = list(_sections.items())

    _all_section_htmls = []
    for _title, _cols in _section_items:
        _all_section_htmls.append(_build_section_table(_title, _cols, _all_selected, _pid_color_map, _df_meta_2b_display))

    _grid_cells = f"""
    <div style="min-width:0;">{_cell_table_html}</div>
    <div style="min-width:0;">{_all_section_htmls[0]}</div>
    """
    for _i in range(1, len(_all_section_htmls), 2):
        _left = _all_section_htmls[_i]
        _right = _all_section_htmls[_i + 1] if _i + 1 < len(_all_section_htmls) else ""
        _grid_cells += f"""
        <div style="min-width:0;">{_left}</div>
        <div style="min-width:0;">{_right}</div>
        """

    mo.stop(_grid_cells.strip() == "", mo.Html('<div style="margin-top:16px; color:#888; font-style:italic;">No metadata available for selected participants.</div>'))

    mo.Html(f"""
    <div style="margin-top:16px;">
        <div style="font-size:15px; font-weight:700; margin-bottom:16px;">Selected Participant Metadata</div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:24px; overflow:visible;">
            {_grid_cells}
        </div>
    </div>
    """)
    return


if __name__ == "__main__":
    app.run()
