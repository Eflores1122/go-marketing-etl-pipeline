#!/usr/bin/env python3
"""Write the synthetic Taller Norte CSVs used by the ETL.

Taller Norte is a fictional home-and-clothing shop. The files are not a
real ad export. The random stream is Random(20260311), so a rerun from the
repository root replaces the CSVs byte for byte:

    python3 scripts/generate_retail.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 20260311
SHOPPERS = 460
ORIGIN = datetime(2024, 3, 4)
SPAN = timedelta(days=28)
ROOT = Path(__file__).resolve().parents[1]

GOOGLE = [
    ("Busqueda | Calzado", "Zapatillas", "zapatilla-urbana"),
    ("Busqueda | Cocina", "Sartenes", "sarten-24"),
    ("Display | Iluminacion", "Lamparas", "lampara-mesa"),
    ("Shopping | Textil", "Mantas", "manta-lana"),
]
FACEBOOK = [
    ("Prospecto | Ropa", "Camisetas", "chaqueta-lino"),
    ("Remarketing | Carrito", "Carrito visto", "almohadon"),
    ("Catalogo | Hogar", "Mesa", "taza-gres"),
]
# Basket totals in cents. They are shelf prices for the fictional shop.
PRICES = {
    "zapatilla-urbana": 7990,
    "sarten-24": 3650,
    "lampara-mesa": 4200,
    "manta-lana": 5400,
    "chaqueta-lino": 9600,
    "almohadon": 2800,
    "taza-gres": 1850,
    "delantal": 2250,
}
DEVICES = ("mobile", "desktop", "tablet")
DEVICE_WEIGHTS = (62, 30, 8)


def money(cents: int) -> str:
    return f"{cents / 100:.2f}"


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def clamp(moment: datetime) -> datetime:
    end = ORIGIN + SPAN - timedelta(seconds=1)
    if moment < ORIGIN:
        return ORIGIN
    if moment > end:
        return end
    return moment


def main() -> None:
    rng = random.Random(SEED)
    google_rows = []
    facebook_rows = []
    site_rows = []
    sessions = 0

    for i in range(SHOPPERS):
        user_id = f"tn-{i:04d}"
        n_sessions = 1 if rng.random() < 0.64 else 2
        for s in range(n_sessions):
            sessions += 1
            session = f"s-{i:04d}-{s}"
            device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
            day = rng.randrange(26)
            start = ORIGIN + timedelta(days=day, minutes=rng.randint(8 * 60, 18 * 60))
            kind = rng.choices(("google", "facebook", "both"), weights=(52, 30, 18), k=1)[0]
            cursor = start
            last_click = start
            sku = "lampara-mesa"

            def click(platform: str, when: datetime) -> datetime:
                nonlocal sku, last_click
                when = clamp(when)
                if platform == "google":
                    campaign, group, sku_name = rng.choice(GOOGLE)
                    sku = sku_name
                    cents = rng.randint(32, 148)
                    google_rows.append(
                        [
                            stamp(when),
                            user_id,
                            session,
                            device,
                            campaign,
                            group,
                            money(cents),
                            "google",
                            f"g-{i:04d}-{s}-{len(google_rows):04d}",
                        ]
                    )
                else:
                    campaign, group, sku_name = rng.choice(FACEBOOK)
                    sku = sku_name
                    cents = rng.randint(28, 132)
                    facebook_rows.append(
                        [
                            stamp(when),
                            user_id,
                            session,
                            device,
                            campaign,
                            group,
                            money(cents),
                            "facebook",
                            f"f-{i:04d}-{s}-{len(facebook_rows):04d}",
                        ]
                    )
                if when >= last_click:
                    last_click = when
                return when

            if kind == "google":
                n = 1 if rng.random() < 0.72 else 2
                for n_click in range(n):
                    click("google", cursor + timedelta(minutes=12 * n_click))
            elif kind == "facebook":
                n = 1 if rng.random() < 0.74 else 2
                for n_click in range(n):
                    click("facebook", cursor + timedelta(minutes=14 * n_click))
            else:
                # The two clicks are ordered, but which platform is first varies.
                first, second = ("google", "facebook")
                gap = rng.randint(25, 160)
                if rng.random() < 0.46:
                    first, second = second, first
                click(first, cursor)
                click(second, cursor + timedelta(minutes=gap))

            view_channel = "google" if kind == "google" else "facebook" if kind == "facebook" else rng.choice(("google", "facebook"))
            n_views = rng.randint(2, 5)
            viewed = cursor
            for n_view in range(n_views):
                viewed = clamp(last_click + timedelta(minutes=3 + n_view * rng.randint(2, 18)))
                site_rows.append(
                    [
                        stamp(viewed),
                        user_id,
                        session,
                        device,
                        "view",
                        '{"path":"/producto/%s"}' % sku,
                        view_channel,
                    ]
                )

            if rng.random() < 0.24:
                cart_at = clamp(viewed + timedelta(minutes=rng.randint(1, 12)))
                site_rows.append(
                    [
                        stamp(cart_at),
                        user_id,
                        session,
                        device,
                        "add_to_cart",
                        '{"sku":"%s","qty":1}' % sku,
                        view_channel,
                    ]
                )
                viewed = cart_at

            purchase_p = 0.10 if kind == "both" else 0.035
            if rng.random() < purchase_p:
                bought = clamp(viewed + timedelta(minutes=rng.randint(4, 50)))
                # Keep the purchase after the latest click so multi-channel
                # sessions satisfy bonus_query.sql when both clicks exist.
                if bought <= last_click:
                    bought = clamp(last_click + timedelta(minutes=5))
                qty = 1 if rng.random() < 0.82 else 2
                value = PRICES[sku] * qty
                site_rows.append(
                    [
                        stamp(bought),
                        user_id,
                        session,
                        device,
                        "purchase",
                        '{"sku":"%s","qty":%d,"value":%s}' % (sku, qty, money(value)),
                        view_channel,
                    ]
                )
            elif rng.random() < 0.035:
                signed = clamp(viewed + timedelta(minutes=rng.randint(1, 8)))
                site_rows.append(
                    [
                        stamp(signed),
                        user_id,
                        session,
                        device,
                        "signup",
                        '{"plan":"cuenta"}',
                        view_channel,
                    ]
                )

    google_rows.sort(key=lambda row: (row[0], row[2], row[8]))
    facebook_rows.sort(key=lambda row: (row[0], row[2], row[8]))
    site_rows.sort(key=lambda row: (row[0], row[2], row[4]))

    write(
        ROOT / "google_clicks.csv",
        ["timestamp", "user_id", "session_key", "device_type", "campaign", "ad_group", "cost", "utm_channel", "gclid"],
        google_rows,
    )
    write(
        ROOT / "facebook_clicks.csv",
        ["timestamp", "user_id", "session_key", "device_type", "campaign", "ad_set", "spend", "utm_channel", "fbclid"],
        facebook_rows,
    )
    write(
        ROOT / "site_events.csv",
        ["timestamp", "user_id", "session_key", "device_type", "action_type", "action_details", "utm_channel"],
        site_rows,
    )
    print(
        f"shoppers={SHOPPERS} sessions={sessions} "
        f"google={len(google_rows)} facebook={len(facebook_rows)} site={len(site_rows)} "
        f"events={len(google_rows) + len(facebook_rows) + len(site_rows)}"
    )


def write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


if __name__ == "__main__":
    main()
