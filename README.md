# CMAPSS: CSV → Kafka → Spark Structured Streaming

Minimalny pipeline lokalny: producer czyta `data/train_FD001.txt`, wysyła rekordy JSON do Kafka, a Spark liczy agregaty 10-sekundowych okien i zapisuje zamknięte okna do Parquet.

## Wymagania

- Docker Desktop z Docker Compose
- Python 3.9–3.12
- Java 8, 11 lub 17 skonfigurowana w `JAVA_HOME` (wymagana przez PySpark 3.5)
- Dostęp do internetu przy pierwszym uruchomieniu Spark, aby pobrać Kafka connector

Uruchamiaj poniższe polecenia z katalogu głównego projektu.

## Instalacja

W PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Jeśli PowerShell blokuje aktywację środowiska, można pominąć aktywację i używać bezpośrednio `.venv\Scripts\python.exe`.

## Kolejność uruchamiania

Potrzebne są trzy terminale.

### Terminal 1 — Kafka

```powershell
docker compose up -d
docker compose ps
```

Kontener powinien mieć status `Up`. Logi:

```powershell
docker compose logs -f kafka
```

Opcjonalnie sprawdź, czy broker odpowiada i czy temat jest widoczny:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Temat `cmapss-telemetry` jest tworzony automatycznie przy pierwszej publikacji.

### Terminal 2 — Spark

Uruchom Spark przed producerem. Consumer subskrybuje temat od najstarszego dostępnego rekordu, więc kolejność nie powoduje utraty danych, ale wcześniejsze uruchomienie ułatwia obserwację.

```powershell
.\.venv\Scripts\Activate.ps1
python src\spark_consumer.py
```

Przy pierwszym uruchomieniu Spark może pobierać pakiet `spark-sql-kafka-0-10` i jego zależności. Nie zamykaj terminala: Structured Streaming działa ciągle i czeka na dane.

### Terminal 3 — Python producer

```powershell
.\.venv\Scripts\Activate.ps1
$env:EVENTS_PER_SECOND = "100"
python src\producer.py
```

`EVENTS_PER_SECOND` jest opcjonalne. Domyślnie wynosi `5`; cały FD001 ma ponad 20 tys. rekordów, więc wtedy wysyłka potrwa około godziny. Ustawienie `100` znacznie skraca test. Po wysłaniu wszystkich rekordów producer kończy działanie; consumer nadal działa.

## Weryfikacja

1. **Kafka:** `docker compose ps` pokazuje broker jako `Up`, a logi nie zawierają błędów startu.
2. **Producer:** terminal pokazuje `Wysłano .../... rekordów`, a na końcu `Producer zakończony.`.
3. **Spark:** po uruchomieniu pojawia się komunikat o zapisie do `output/spark_results`. Pierwsze wyniki pojawią się po zamknięciu okien przez watermark (około 30 sekund opóźnienia event-time plus czas przetwarzania).
4. **Parquet:** po przetworzeniu danych w `output/spark_results` pojawią się pliki `part-*.parquet`; `output/spark_checkpoint` przechowuje offsety i stan strumienia.

Odczyt zapisanych wyników z kolejnego PowerShella:

```powershell
.\.venv\Scripts\python.exe -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('CheckOutput').getOrCreate(); s.read.parquet('output/spark_results').show(20, truncate=False); s.stop()"
```

Wynik zawiera `window` (początek i koniec okna), `unit_number` oraz średnie pomiary sensorów 2–4. `sensor_2_stddev` może być pusty (`null`), jeśli w danym oknie dla silnika jest tylko jeden pomiar.

## Co robi kod

- Plik train ma 26 kolumn rozdzielonych białymi znakami: numer silnika, cykl, 3 ustawienia operacyjne i 21 sensorów. Producer nadaje nazwy kolumn, sprawdza brak pustych wartości i wysyła każdy rekord jako JSON.
- Klucz Kafka to tekstowa wartość `unit_number`, dzięki czemu rekordy tego samego silnika trafiają do tej samej partycji.
- `event_timestamp` jest wysyłany jako tekst ISO-8601 UTC i parsowany przez Spark jako `TIMESTAMP`. To naprawia niezgodność wcześniejszego formatu milisekund z typem oczekiwanym przez watermark.
- Spark grupuje rekordy po silniku i oknie 10 sekund. Watermark 30 sekund pozwala obsłużyć spóźnione dane; sink Parquet działa z `append`, zapisując dopiero zamknięte okna. W przypadku jednorazowego, skończonego pliku ostatnie okna (do 30 sekund event-time) mogą pozostać niezapisane, bo po zakończeniu producer nie wysyła już danych przesuwających watermark. To normalna semantyka streamingu; długotrwały strumień zapisze je po nadejściu kolejnych rekordów.
- `startingOffsets=earliest` stosuje się przy pierwszym uruchomieniu. Przy kolejnych startach checkpoint w `output/spark_checkpoint` powoduje wznowienie od zapisanych offsetów, bez ponownego czytania całego tematu.

## Konfiguracja

Domyślne wartości można nadpisać zmiennymi środowiskowymi:

| Zmienna | Domyślnie | Znaczenie |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Adres brokera widoczny z hosta |
| `KAFKA_TOPIC` | `cmapss-telemetry` | Temat Kafka |
| `DATA_PATH` | `data/train_FD001.txt` | Plik wejściowy |
| `EVENTS_PER_SECOND` | `5` | Tempo publikowania |
| `OUTPUT_PATH` | `output/spark_results` | Katalog danych Parquet |
| `CHECKPOINT_PATH` | `output/spark_checkpoint` | Katalog checkpointu Spark |

Po zmianie tematu lub ponownym uruchomieniu od początku użyj nowej ścieżki `CHECKPOINT_PATH`. Nie usuwaj checkpointu, gdy Spark nadal działa.

## Typowe problemy

- **Docker/Kafka nie startuje lub port 9092 jest zajęty:** sprawdź `docker compose ps` i `docker compose logs kafka`; zamknij usługę używającą portu 9092 albo zmień mapowanie portów wraz z `KAFKA_BOOTSTRAP`.
- **Producer zgłasza błąd połączenia:** broker musi działać, a z hosta używaj `localhost:9092`. Sprawdź `KAFKA_BOOTSTRAP` i logi kontenera.
- **`ModuleNotFoundError` dla `kafka` lub `pyspark`:** aktywuj `.venv` i ponownie wykonaj `python -m pip install -r requirements.txt`.
- **Spark nie znajduje źródła `kafka`:** sprawdź dostęp do internetu oraz wersję PySpark. Consumer pobiera connector dla Spark 3.5.1 i Scala 2.12 przy starcie.
- **`JAVA_HOME` lub Java nie znaleziona:** zainstaluj JDK 8, 11 albo 17, ustaw `JAVA_HOME` i otwórz nowy terminal.
- **Brak plików Parquet zaraz po starcie:** uruchom producer i poczekaj na zamknięcie okien przez watermark; upewnij się też, że producer wysłał rekordy. Przy jednorazowym pliku jego końcowe okna nie będą zatwierdzone bez kolejnych zdarzeń. Dla pełnego batchowego wyniku trzeba osobno opracować obsługę końca wejścia lub użyć batchowego odczytu.
- **Brak ponownego przetwarzania przy kolejnym uruchomieniu:** to oczekiwane działanie checkpointu. Aby wykonać nowy test od początku, zatrzymaj Spark i ustaw nową ścieżkę `CHECKPOINT_PATH` (oraz osobną `OUTPUT_PATH` dla oddzielnego wyniku).
- **`FileNotFoundError` dla danych:** uruchom polecenia z katalogu projektu lub ustaw `DATA_PATH` na właściwy plik.

Zatrzymanie: przerwij Spark i producer przez `Ctrl+C`, a Kafka przez `docker compose down`.
