#!/usr/bin/env python3

import argparse
from pathlib import Path

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Grant/revoke orecchio site write access for a Firebase Auth UID."
    )
    p.add_argument("--service-account", required=True, help="Path to Firebase service account JSON")
    p.add_argument("--db-url", required=True, help="Realtime Database URL")
    p.add_argument("--site-id", required=True, help="Site ID, e.g. wytheville-01")
    p.add_argument("--uid", required=True, help="Firebase Auth UID")
    p.add_argument(
        "--revoke",
        action="store_true",
        help="Revoke access instead of granting",
    )
    return p.parse_args()


def init_app(service_account: Path, db_url: str):
    app_name = f"orecchio-site-writer-{service_account.resolve()}"
    for app in firebase_admin._apps.values():
        if app.name == app_name:
            return app
    cred = credentials.Certificate(str(service_account))
    return firebase_admin.initialize_app(cred, {"databaseURL": db_url}, name=app_name)


def main() -> None:
    args = parse_args()
    service_account = Path(args.service_account)
    if not service_account.exists():
        raise SystemExit(f"service account not found: {service_account}")

    app = init_app(service_account, args.db_url.rstrip("/"))
    ref = db.reference(f"site_writers/{args.site_id}/{args.uid}", app=app)
    if args.revoke:
        ref.delete()
        print(f"revoked: site_id={args.site_id} uid={args.uid}")
    else:
        ref.set(True)
        print(f"granted: site_id={args.site_id} uid={args.uid}")


if __name__ == "__main__":
    main()
