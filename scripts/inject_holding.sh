#!/usr/bin/env bash
# Valide la détection HOLDING_PATTERN du job Spark sans dépendre du trafic réel.
#
# Injecte dans flight-positions 5 positions d'un avion fictif quasi immobile
# (écart max 0,01°, ~3000 m) près de Casablanca, une toutes les 120 s, avec
# timestamp = heure d'envoi. Les messages partent en temps réel plutôt qu'en
# rafale : des timestamps futurs feraient avancer le watermark et le trafic
# réel serait rejeté comme tardif.
#
# L'horloge locale doit être à l'heure : le script s'arrête si elle s'écarte de
# plus de 60 s de celle d'OpenSky.
#
# Les fenêtres font 10 min et glissent de 5 min : pour que les 5 points (8 min)
# tombent dans la même fenêtre, le script démarre au plus 100 s après une
# frontière de 5 min, et attend la suivante sinon.
#
# Résultat attendu (sortie console de flight-spark) : une seule ligne
# HOLDING_PATTERN pour l'icao24 injecté, nb_positions=5, émise ~2 min après la
# fin de la fenêtre (watermark). Les fenêtres voisines ont moins de 4 points et
# ne doivent rien émettre.
#
# Usage :   scripts/inject_holding.sh [icao24]     (défaut : test01)
# Suivi :   docker logs -f flight-spark 2>&1 | grep HOLDING_PATTERN
# Nettoyage Mongo après le test :
#   docker exec flight-mongo mongosh flights --quiet \
#     --eval 'db.positions.deleteMany({icao24: "test01"})'
set -u
# Git Bash convertit /opt/... en chemin Windows dans les arguments de docker exec
export MSYS_NO_PATHCONV=1

ICAO24="${1:-test01}"
LATS=(33.370 33.375 33.380 33.375 33.370)
LONS=(-7.580 -7.575 -7.570 -7.575 -7.580)
ALTS=(3000 3020 2990 3010 3000)

# Les timestamps viennent de l'horloge locale, ceux du trafic réel d'OpenSky :
# un décalage fait passer les messages injectés pour tardifs (watermark) et
# Spark les ignore sans erreur.
ref=$(curl -sI --max-time 10 https://opensky-network.org | tr -d '\r' | sed -n 's/^[Dd]ate: //p')
if [ -n "$ref" ]; then
  skew=$(( $(date -u -d "$ref" +%s) - $(date -u +%s) ))
  if [ "${skew#-}" -gt 60 ]; then
    echo "horloge locale décalée de ${skew}s par rapport à OpenSky : synchroniser l'heure avant le test" >&2
    exit 1
  fi
else
  echo "avertissement : impossible de vérifier l'horloge auprès d'OpenSky" >&2
fi

now=$(date -u +%s)
off=$(( now % 300 ))
if [ "$off" -gt 100 ]; then
  wait=$(( 300 - off + 5 ))
  echo "attente ${wait}s jusqu'à la prochaine frontière de 5 min"
  sleep "$wait"
fi

t0=$(date -u +%s)
for i in 0 1 2 3 4; do
  target=$(( t0 + i * 120 )); d=$(( target - $(date -u +%s) )); [ "$d" -gt 0 ] && sleep "$d"
  ts=$(date -u +%s)
  msg="{\"icao24\":\"${ICAO24}\",\"callsign\":\"TESTHLD\",\"country\":\"Test\",\"timestamp\":${ts},\"longitude\":${LONS[$i]},\"latitude\":${LATS[$i]},\"altitude_m\":${ALTS[$i]},\"on_ground\":false,\"speed_kmh\":420.0,\"vertical_rate_ms\":0.0,\"squawk\":\"1000\"}"
  out=$(echo "$msg" | docker exec -i flight-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic flight-positions 2>&1)
  if [ $? -eq 0 ]; then
    echo "envoyé #$((i+1)) ts=${ts} ($(date -u -d @${ts} +%H:%M:%S))"
  else
    echo "ÉCHEC envoi #$((i+1)): $(echo "$out" | tail -2 | tr '\n' ' ')"
  fi
done
echo "injection terminée"
