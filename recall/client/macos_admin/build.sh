#!/bin/sh
set -eu

SOURCE_ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
OUTPUT=${1:-"$SOURCE_ROOT/dist"}
APP="$OUTPUT/Recall Brain.app"

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp "$SOURCE_ROOT/Info.plist" "$APP/Contents/Info.plist"
xcrun swiftc \
  -parse-as-library \
  -O \
  -warnings-as-errors \
  -framework SwiftUI \
  -framework AuthenticationServices \
  -framework CryptoKit \
  -framework Security \
  "$SOURCE_ROOT/RecallBrainAdmin.swift" \
  -o "$APP/Contents/MacOS/RecallBrainAdmin"
chmod 0755 "$APP/Contents/MacOS/RecallBrainAdmin"
codesign --force --sign - --timestamp=none "$APP"
SELF_TEST=$("$APP/Contents/MacOS/RecallBrainAdmin" --self-test)
case "$SELF_TEST" in
  *'"status":"pass"'*) ;;
  *) echo "native self-test failed" >&2; exit 1 ;;
esac
printf '{"app":"Recall Brain.app","architecture":"%s","self_test":"pass","status":"built"}\n' "$(uname -m)"
