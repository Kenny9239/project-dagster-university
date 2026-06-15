import dagster as dg
from dagster_essentials.defs.assets import constants # adjust import path to your project's constants module

import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


@dg.asset
def rainfall_hour_report() -> None:
    """
    Fetches hourly rainfall XML data, parses it, and writes the
    RainHour HTML report to local storage.
    """
    rain_fall, finish_time_fmt = parse_xml_et_rain(constants.RAIN_DATA_URL)

    generate_html(
        rain_fall,
        finish_time_fmt,
        output_file=constants.RAIN_HOUR_OUTPUT_FILE_PATH,
    )





# ---------------------------------------------------------------------------
# Colour-level helpers (mirrors RainHour.htm CSS classes)
# ---------------------------------------------------------------------------

# Level classes for individual hourly cells (yellow → orange → red scale)
# Thresholds (mm): 0, >0, >=1, >=2, >=3, >=5, >=7, >=10, >=15, >=20, >=30, >=40
LEVEL_THRESHOLDS = [40, 30, 20, 15, 10, 7, 5, 3, 2, 1, 0]  # descending


def get_level_class(value: float) -> str:
    """Return LevelN CSS class for an hourly rainfall value."""
    for i, threshold in enumerate(LEVEL_THRESHOLDS):
        if value >= threshold and value > 0:
            return f"Level{11 - i}"
    return "Level1"  # zero or effectively zero


# Total classes for the site-total cell (blue intensity scale)
# Thresholds (mm): 0, >0, >=2, >=5, >=10, >=20, >=30, >=40, >=60, >=80, >=100, >=150, >=200
TOTAL_THRESHOLDS = [200, 150, 100, 80, 60, 40, 30, 20, 10, 5, 2, 0]  # descending


def get_total_class(value: float) -> str:
    """Return TotalN CSS class for a site 24-hour total."""
    for i, threshold in enumerate(TOTAL_THRESHOLDS):
        if value >= threshold and value > 0:
            return f"Total{12 - i}"
    return "Total"  # zero


# ---------------------------------------------------------------------------
# XML parsing  (unchanged logic from original)
# It will retuen a nested dict in the follwoing structure plus the finishing time in the format "dd/mm/yyyy HH:MM" formate
#
#[
#    {
#        'site': 'Akatarawa River at Cemetery',   # str  — SiteName attribute
#        'site_total': 63.5,                       # float — sum of all I1 values, rounded to 2dp
#        'records': {
#            '2026-06-04T14:00:00': 0.0,           # str timestamp -> float rainfall (mm)
#            '2026-06-04T15:00:00': 1.0,
#            '2026-06-04T16:00:00': 0.2,
#            # ... one entry per <E> element
#            '2026-06-05T13:00:00': 6.2,
#        }
#    },
#    {
#        'site': 'Akatarawa River at Warwicks',
#        'site_total': 70.0,
#        'records': { ... }
#    },
#    # ... one dict per <Measurement> element
#]
# ---------------------------------------------------------------------------

def parse_xml_et_rain(source):
    """Parse rainfall XML from a URL or a local file path/file-like object."""
    if isinstance(source, str) and source.lower().startswith(("http://", "https://")):
        req = urllib.request.Request(source, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as response:
            tree = ET.parse(response)
    else:
        tree = ET.parse(source)

    root = tree.getroot()

    rain_fall = []
    finish_time_fmt = None

    for measurement in root.findall('Measurement'):
        site_name = measurement.attrib['SiteName']

        data = measurement.find('Data')
        if data is None:
            continue

        records = data.findall('E')
        if not records:
            continue

        site_total = 0.0
        site_records = {}

        for record in records:
            timestamp = record.find('T').text
            rainfall = float(record.find('I1').text)
            site_records[timestamp] = rainfall
            site_total += rainfall

        rain_fall.append({
            'site': site_name,
            'site_total': round(site_total, 2),
            'records': site_records,
        })

        # Track finishing time from the last record of the last site processed
        finish_time = records[-1].find('T').text
        dt = datetime.fromisoformat(finish_time)
        finish_time_fmt = dt.strftime("%d/%m/%Y %H:%M")

    return rain_fall, finish_time_fmt


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

CSS = """\
BODY  { background-color: #FFFFFF }
TH    { font-family:Arial; font-size: 8pt; color:#212121; text-align: Left;
        font-weight: bold; background-color: #F5F5F5; }
TD    { font-family:Tahoma; font-size: 8pt; color:#212121; text-align: Right;
        font-weight: normal; }
.Site   { text-align: Left; Width: 200pt; font-weight: normal; background-color: #F5F5F5 }
.Level1  { text-align: Right; font-weight: normal; background-color: #FFF9D0; width: 18pt; }
.Level2  { text-align: Right; font-weight: normal; background-color: #FFEFAC; width: 18pt; }
.Level3  { text-align: Right; font-weight: normal; background-color: #FFE689; width: 18pt; }
.Level4  { text-align: Right; font-weight: normal; background-color: #FFDC65; width: 18pt; }
.Level5  { text-align: Right; font-weight: normal; background-color: #FFD342; width: 18pt; }
.Level6  { text-align: Right; font-weight: normal; background-color: #FFC91E; width: 18pt; }
.Level7  { text-align: Right; font-weight: normal; background-color: #FEA918; width: 18pt; }
.Level8  { text-align: Right; font-weight: normal; background-color: #FD8912; width: 18pt; }
.Level9  { text-align: Right; font-weight: normal; background-color: #FD6A0C; width: 18pt; }
.Level10 { text-align: Right; font-weight: normal; background-color: #FC4A06; width: 18pt; }
.Level11 { text-align: Right; font-weight: normal; background-color: #FB2A00; width: 18pt; }
.Total   { text-align: Right; Width: 22pt; font-weight: normal; background-color: #FFFFFF; }
.Total1  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #F5FAFF; }
.Total2  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #EBF5FF; }
.Total3  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #E0F0FF; }
.Total4  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #D6EBFF; }
.Total5  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #CCE6FF; }
.Total6  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #C2E0FF; }
.Total7  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #B8DBFF; }
.Total8  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #A0CFEC; }
.Total9  { text-align: Right; Width: 22pt; font-weight: normal; background-color: #A3D1FF; }
.Total10 { text-align: Right; Width: 22pt; font-weight: normal; background-color: #99CCFF; }
.Total11 { text-align: Right; Width: 22pt; font-weight: normal; background-color: #8AB8E6; }
.Total12 { text-align: Right; Width: 22pt; font-weight: normal; background-color: #7AA3CC; }
H1 { margin-bottom: 2pt; font-family:Arial; font-size: 12pt; color: #212121; text-align: center }
H2 { font-family:Arial; font-size: 8pt; color: #212121; text-align: center;
     margin-bottom: 2pt; margin-top: 2pt; margin-right: 9pt; }
"""


def format_value(v: float) -> str:
    """
    Format a rainfall value: drop trailing .0 for whole numbers.
    e.g.:
        v = 2.6      → "2.6"
        v = 2.67     → "2.7"   # rounds up
        v = 2.64     → "2.6"   # rounds down
        v = 2.0      → "2.0"   # always shows the .0
        v = 10.0     → "10.0"
    """
    return str(int(v)) if v == int(v) else f"{v:.1f}"


def generate_html(rain_fall, finish_time_fmt, output_file="RainHour_output.htm"):
    # Build a canonical 24-hour window ending at the latest timestamp.
    # Using a union of all timestamps would pull in stale/historical records
    # from sites whose data falls outside the current reporting window.
    latest_dt = max(
        datetime.fromisoformat(ts)
        for site in rain_fall
        for ts in site['records']
    )
    all_timestamps = [
        (latest_dt - timedelta(hours=i)).isoformat()
        for i in range(23, -1, -1)
    ]

    # Build header labels (HH:MM only)
    hour_labels = [
        datetime.fromisoformat(ts).strftime("%H:%M") for ts in all_timestamps
    ]

    num_hours = len(all_timestamps)

    lines = []
    lines.append("<html>")
    lines.append("<head>")
    lines.append('<META http-equiv="Content-Type" content="text/html">')
    lines.append(f"<style type=\"text/css\">\n{CSS}</style>")
    lines.append("</head>")
    lines.append("<body>")
    lines.append(
        f'<H1>Hourly Rainfall (mm) Finishing at {finish_time_fmt} (NZST)</H1>'
    )
    lines.append(
        '<table align="Center" border="1" cellpadding="2" '
        'cellspacing="0" BorderColor="#0">'
    )

    # ---- Header row 1: colspan labels ----
    lines.append("<tr>")
    lines.append('<th colspan="2"> </th>')
    lines.append(f'<th colspan="{num_hours}">')
    lines.append('<div style="float: left;">Oldest Data</div>')
    lines.append('<div style="float: right;">Most Recent Data</div>')
    lines.append("</th>")
    lines.append("</tr>")

    # ---- Header row 2: column names ----
    lines.append("<th>Site Name</th>")
    lines.append("<th>Total</th>")
    for label in hour_labels:
        lines.append(f"<th>{label}</th>")

    # ---- Data rows ----
    for site in rain_fall:
        total = site['site_total']
        total_class = get_total_class(total)
        total_display = format_value(total)

        lines.append("<tr>")
        lines.append(f'<td class="Site">{site["site"]}</td>')
        lines.append(f'<td class="{total_class}">{total_display}</td>')

        for ts in all_timestamps:
            rainfall = site['records'].get(ts, 0.0)
            level_class = get_level_class(rainfall)
            lines.append(f'<td class="{level_class}">{format_value(rainfall)}</td>')

        lines.append("</tr>")

    lines.append("</table>")
    lines.append("</body>")
    lines.append("</html>")

    html_content = "\n".join(lines)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"HTML report written to: {output_file}")
    return output_file

"""
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    DATA_URL = "https://hilltop.gw.govt.nz/Data.hts?Service=Hilltop&Request=GetData&Collection=Rainfall&TimeInterval=P24H&Method=Total&Interval=1hours&Alignment=00:00"  # <-- set the real URL here

    rain_fall, finish_time_fmt = parse_xml_et_rain(DATA_URL)
    generate_html(rain_fall, finish_time_fmt, output_file="RainHour_output_from_url.htm")
    print(f"Hourly Rainfall (mm) Finishing at {finish_time_fmt} (NZST)")

"""