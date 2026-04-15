#!/bin/bash
# ═══════════════════════════════════════════════════════
# Setup daglig sync för Trav-modellen
# Kör detta EN gång för att sätta upp automatisk körning
# ═══════════════════════════════════════════════════════

PLIST_NAME="com.trav.daily-sync"
PLIST_PATH="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"
SCRIPT_PATH="/Users/dennisdemirtok/trading_agent/daily_sync.py"
LOG_PATH="/Users/dennisdemirtok/trading_agent/data/launchd_sync.log"

echo "═══════════════════════════════════════════"
echo "  Trav-modellen — Setup daglig sync"
echo "═══════════════════════════════════════════"
echo ""

# Fråga efter Supabase-config
read -p "Supabase URL (t.ex. https://xxx.supabase.co): " SUPA_URL
read -p "Supabase anon/service key: " SUPA_KEY

if [ -z "$SUPA_URL" ] || [ -z "$SUPA_KEY" ]; then
    echo ""
    echo "⚠️  Supabase ej konfigurerad — sync kör bara lokalt (SQLite)"
    echo "   Du kan lägga till det senare i: $PLIST_PATH"
    SUPA_URL=""
    SUPA_KEY=""
fi

# Skapa plist
cat > "$PLIST_PATH" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>${SCRIPT_PATH}</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
        <key>SUPABASE_URL</key>
        <string>${SUPA_URL}</string>
        <key>SUPABASE_KEY</key>
        <string>${SUPA_KEY}</string>
    </dict>

    <!-- Kör 18:30 varje dag (efter börsens stängning 17:30) -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>18</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>${LOG_PATH}</string>
    <key>StandardErrorPath</key>
    <string>${LOG_PATH}</string>

    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
EOF

echo ""
echo "✓ Plist skapad: $PLIST_PATH"

# Ladda schemat
launchctl unload "$PLIST_PATH" 2>/dev/null
launchctl load "$PLIST_PATH"

echo "✓ Schema laddat — kör dagligen 18:30"
echo ""
echo "═══════════════════════════════════════════"
echo "  Kommandon:"
echo "  Kör manuellt:   python3 $SCRIPT_PATH"
echo "  Se loggar:      cat $LOG_PATH"
echo "  Stoppa schema:  launchctl unload $PLIST_PATH"
echo "  Starta schema:  launchctl load $PLIST_PATH"
echo "═══════════════════════════════════════════"
