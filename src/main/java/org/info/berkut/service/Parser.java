package org.info.berkut.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Service
@Slf4j
@RequiredArgsConstructor
public class Parser {

    private final ObjectMapper mapper = new ObjectMapper();

    private static final String BASE = "https://center.berkut";
    private static final String UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome";

    @Value("${berkut.secure.login}")
    private String username;

    @Value("${berkut.secure.password}")
    private String password;

    @Value("${berkut.client.id:acme}")
    private String clientId;

    @Value("${berkut.client.secret:acmesecret}")
    private String clientSecret;

    @Value("${berkut.redirect-uri:https://center.berkut/app/ru/}")
    private String redirectUri;

    private String sessionCookie = "";
    private String currentToken;

    private final WebClient webClient;
    private static final String HEADER = "id|taxpayer_name_latin|taxpayer_name_cyrillic|taxpayer_name_original|sex|taxpayer_birthday|citizenship|taxpayer_iin_bin|taxpayer_personal_number|photography_refusing_reason|intersection_status|death_date|death_country|death_reg_place|death_add_information|document_number|document_type|document_issue_date|document_validity_period|document_issue_country|document_issuing_authority|duty_officer_decision|duty_officer_decision_date|duty_officer_decision_add_info|date|detention_place|police_nariad|decision_making_body|decison_making_authority|place_of_birth|location|place_of_work|family_information|education|supression_date|checkpoint|entry_exit_place|trip_purpose|border_crossing_method|direction|exit_country|departure_point|entry_country|destination_pint|system_number|create_date|creater|source|serial_number|status|flight_train_number|belonging|flight_class|flight_type|flight_transport_vessel_number|flight_date_fact|flight_date_plan|vin_code|trailer_number|mark_type|colour|owner|vessel_name|home_port|carriage_quantity|foreign_carriage_quantity|visa_frequency|visa_category|visa_type|visa_number|visa_start_date|visa_expiration_date|document_number1|iin|start_date|expiration_date|actual_date";

    private static final Random random = new Random();
    private final List<String> globalCsvLines = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean exportRunning = false;

    // PUBLIC API
    public Mono<String> exportCsv(String dateFrom, String dateTo, int startPage) {
        return authenticateAndGetToken()
                .doOnNext(token -> currentToken = token)
                .flatMap(token -> requestData(dateFrom, dateTo, startPage));
    }

    private Mono<String> authenticateAndGetToken() {
        log("=== STEP 0: GET /auth-service/login ===");
        return fetchLoginPage()
                .then(Mono.defer(() -> {
                    log("=== STEP 1: POST /auth-service/login → get code ===");
                    return submitLoginForm();
                }))
                .flatMap(code -> {
                    log("AUTH CODE = " + code);
                    log("=== STEP 2: POST /oauth/token → get access_token ===");
                    return exchangeCodeForToken(code);
                });
    }

    private Mono<Void> fetchLoginPage() {
        return webClient.get()
                .uri(BASE + "/auth-service/login")
                .header(HttpHeaders.USER_AGENT, UA)
                .header(HttpHeaders.ACCEPT, "text/html")
                .exchangeToMono(resp -> {
                    extractCookies(resp);
                    log("SESSION COOKIE after GET /login: " + sessionCookie);
                    if (!resp.statusCode().is2xxSuccessful()) {
                        return Mono.error(new RuntimeException("GET /login failed: " + resp.statusCode()));
                    }
                    return Mono.empty();
                });
    }

    private Mono<String> submitLoginForm() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", username);
        form.add("password", password);
        form.add("approve", "on");

        return webClient.post()
                .uri(BASE + "/auth-service/login")
                .header(HttpHeaders.COOKIE, sessionCookie)
                .header(HttpHeaders.USER_AGENT, UA)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .exchangeToMono(resp -> {
                    extractCookies(resp);
                    log("SESSION after POST /login: " + sessionCookie);

                    List<String> loc = resp.headers().header(HttpHeaders.LOCATION);
                    if (loc != null && !loc.isEmpty() && loc.get(0).contains("code=")) {
                        return Mono.just(extractCode(loc.get(0)));
                    }
                    return Mono.error(new RuntimeException("Authorization code not found after login!"));
                });
    }

    private Mono<String> exchangeCodeForToken(String code) {
        String basic = "Basic " + Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

        return webClient.post()
                .uri(BASE + "/auth-service/oauth/token")
                .header(HttpHeaders.COOKIE, sessionCookie)
                .header(HttpHeaders.AUTHORIZATION, basic)
                .header(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(HttpHeaders.ACCEPT, "application/json")
                .body(BodyInserters.fromFormData("grant_type", "authorization_code")
                        .with("scope", "openid")
                        .with("redirect_uri", redirectUri)
                        .with("code", code))
                .retrieve()
                .bodyToMono(String.class)
                .map(body -> {
                    try {
                        Map<String, Object> map = mapper.readValue(body, new TypeReference<>() {});
                        return (String) map.get("access_token");
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Failed to parse access token", e);
                    }
                });
    }

    private Mono<String> requestData(String from, String to, int startPageUser) {
        int startPage = Math.max(0, startPageUser - 1);

        String requestBody = """
        {
          "fields":[
            {
              "name":"statusDatetime",
              "compareOperator":"BETWEEN",
              "compareValues":["%s","%s"]
            }
          ]
        }
        """.formatted(from, to);

        log("Requesting data...");

        Path baseDir = Paths.get("").toAbsolutePath();
        Path exportDir = baseDir.resolve("export");
        try {
            Files.createDirectories(exportDir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create export folder", e);
        }

        Path finalFile = exportDir.resolve("result.csv");

        exportRunning = true;
        globalCsvLines.clear();
        globalCsvLines.add(HEADER);

        AtomicInteger processedPages = new AtomicInteger(0);
        AtomicInteger totalPagesRef = new AtomicInteger(1);

        Function<Integer, Mono<JsonNode>> fetchPage = page -> webClient.post()
                .uri(BASE + "/pp-center-service/api/crossing-facts/search?page=" + page + "&size=200")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + currentToken)
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .header(HttpHeaders.COOKIE, sessionCookie)
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(String.class)
                .map(body -> {
                    try {
                        log("Processing page: " + page);
                        return mapper.readTree(body);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
                        .maxBackoff(Duration.ofMinutes(2))
                        .filter(ex -> {
                            if (ex instanceof WebClientResponseException wcre) {
                                int status = wcre.getStatusCode().value();
                                return status == 401 || status == 403 || status == 400 ||
                                        status == 500 || status == 501 || status == 502;
                            }
                            return false;
                        })
                        .doBeforeRetryAsync(signal -> {
                            Throwable ex = signal.failure();
                            if (ex instanceof WebClientResponseException wcre) {
                                int status = wcre.getStatusCode().value();
                                if (status == 500 || status == 501 || status == 502) {
                                    log("⚠ Server error 500 — retrying (attempt " + (signal.totalRetries() + 1) + ")");
                                    return Mono.empty();
                                } else if (status == 401 || status == 403 || status == 400) {
                                    log("Token expired — re-authenticating (attempt " + (signal.totalRetries() + 1) + ")");
                                    return authenticateAndGetToken()
                                            .doOnNext(token -> currentToken = token)
                                            .then();
                                }
                            }
                            return Mono.empty();
                        }));

        return fetchPage.apply(startPage)
                .flatMap(first -> {
                    JsonNode pageInfo = first.path("page");
                    int totalPages = pageInfo.path("totalPages").asInt(1);
                    int totalElements = pageInfo.path("totalElements").asInt(0);

                    totalPagesRef.set(totalPages);
                    log("Total records: " + totalElements + ", Total pages: " + totalPages);

                    if (totalPages == 0) return Mono.just("");

                    processPageData(first, globalCsvLines);
                    processedPages.incrementAndGet();

                    return Flux.range(startPage + 1, totalPages - (startPage + 1))
                            .delayElements(Duration.ofMillis(400 + random.nextInt(600)))
                            .concatMap(page -> fetchPage.apply(page)
                                    .doOnNext(json -> {
                                        processPageData(json, globalCsvLines);
                                        int done = processedPages.incrementAndGet();
                                        if (done % 50 == 0) {
                                            saveTempFile(exportDir, page);
                                        }
                                    }))
                            .then(Mono.fromCallable(() -> {
                                Files.write(finalFile, globalCsvLines, StandardCharsets.UTF_8,
                                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                                log("✔ Final file saved: " + finalFile.toAbsolutePath());
                                return String.join("\n", globalCsvLines);
                            }));
                })
                .onErrorResume(e -> {
                    log("FATAL ERROR: " + e.getMessage());
                    return Mono.just(String.join("\n", globalCsvLines));
                });
    }
    private void saveTempFile(Path exportDir, int page){
        page++;
        try {
            Path file = exportDir.resolve("progress_page_" + page + ".csv");
            Files.write(file, globalCsvLines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            log("💾 Auto-saved CSV at page " + page + ": " + file.toAbsolutePath());

        } catch (Exception e) {
            log("ERROR saving temp CSV: " + e.getMessage());
        }
    }

    // -----------------------------
    // DATA PROCESSING
    // -----------------------------
    private void processPageData(JsonNode root, List<String> csvLines) {
        JsonNode items = root.path("_embedded").path("content");

        if (!items.isArray() || items.size() == 0) {
            log("Warning: Page has no items");
            return;
        }

        for (JsonNode item : items) {
            // Document info
            JsonNode doc = item.path("document");
            String surnameEn = getOrEmpty(doc, "surnameEn");
            String givenNameEn = getOrEmpty(doc, "givenNameEn");
            String fathersNameEn = getOrEmpty(doc, "fathersNameEn");
            String surnameRu = getOrEmpty(doc, "surnameRu");
            String givenNameRu = getOrEmpty(doc, "givenNameRu");
            String fathersNameRu = getOrEmpty(doc, "fathersNameRu");
            String surnameOrigin = getOrEmpty(doc, "surnameOrigin");
            String givenNameOrigin = getOrEmpty(doc, "givenNameOrigin");
            String fathersNameOrigin = getOrEmpty(doc, "fathersNameOrigin");

            // Trip info
            JsonNode trip = item.path("trip");

            // Visa info
            JsonNode visa = item.path("crossingFactVisa");

            String[] values = {
                    getOrEmpty(item, "id"),
                    formatName(givenNameEn, surnameEn, fathersNameEn),
                    formatName(givenNameRu, surnameRu, fathersNameRu),
                    formatName(givenNameOrigin, surnameOrigin, fathersNameOrigin),
                    getNestedOrEmpty(doc, "gender"),
                    getOrEmpty(doc, "birthDate"),
                    getNestedOrEmpty(item, "citizenCountry", "valueRu"),
                    getOrEmpty(doc, "iin"),
                    getOrEmpty(doc, "personalNumber"),
                    getNestedOrEmpty(item, "refusePhotoReason", "valueRu"),
                    getOrEmpty(item, "statusValue"),
                    getOrEmpty(item, "deathDate"),
                    getNestedOrEmpty(item, "deathCountry", "valueRu"),
                    getOrEmpty(item, "deathRegistrationPlace"),
                    getOrEmpty(item, "deathAdditionalInfo"),
                    getOrEmpty(doc, "documentNumber"),
                    getNestedOrEmpty(doc, "documentType", "valueRu"),
                    getOrEmpty(doc, "beginDate"),
                    getOrEmpty(doc, "endDate"),
                    getNestedOrEmpty(doc, "issueCountry", "valueRu"),
                    getOrEmpty(doc, "issueAuthority"),
                    getNestedOrEmpty(item, "violationsInfo", "decisionInfo", "valueRu"),
                    getOrEmpty(item, "violationsInfo", "decisionDate"),
                    getOrEmpty(item, "violationsInfo", "additionalInfo"),
                    getOrEmpty(item, "statusDatetime"),
                    getNestedOrEmpty(item, "detentionInfo", "detentionPlace"),
                    getNestedOrEmpty(item, "detentionInfo", "policeSquad"),
                    getNestedOrEmpty(item, "detentionInfo", "decisionMakingBody", "valueRu"),
                    getNestedOrEmpty(item, "detentionInfo", "decisionMakingAuthority"),
                    getOrEmpty(item, "birthPlace"),
                    getOrEmpty(item, "residencePlace"),
                    getOrEmpty(item, "workPlace"),
                    getOrEmpty(item, "familyInfo"),
                    getOrEmpty(item, "training"),
                    "",
                    getNestedOrEmpty(item, "checkpoint", "valueRu"),
                    getNestedOrEmpty(item, "checkpoint", "valueRu"),
                    getNestedOrEmpty(item, "tripPurpose", "valueRu"),
                    getOrEmpty(trip, "crossingFactTypeValue"),
                    getOrEmpty(trip, "directionTypeValue"),
                    getNestedOrEmpty(trip, "fromDestinationCountry", "valueRu"),
                    getOrEmpty(trip, "fromDestinationData"),
                    getNestedOrEmpty(trip, "toDestinationCountry", "valueRu"),
                    getOrEmpty(trip, "toDestinationData"),
                    getOrEmpty(item, "id"),
                    getOrEmpty(item, "createdDatetime"),
                    getOrEmpty(item, "userInfo", "fullName"),
                    getNestedOrEmpty(item, "source", "valueRu"),
                    getOrEmpty(item, "deviceSerialNumber"),
                    getOrEmpty(item, "status"),
                    getOrEmpty(trip, "tripNumber"),
                    getNestedOrEmpty(trip, "ownerCountry", "valueRu"),
                    getNestedOrEmpty(trip, "tripClass", "valueRu"),
                    getNestedOrEmpty(trip, "tripType", "valueRu"),
                    getOrEmpty(trip, "transportNumber"),
                    getOrEmpty(trip, "actualDatetime"),
                    "",
                    "",
                    getOrEmpty(trip, "trailerNumber"),
                    getNestedOrEmpty(trip, "brand", "valueRu"),
                    getNestedOrEmpty(trip, "colorInfo", "valueRu"),
                    getNestedOrEmpty(trip, "ownerCountry", "valueRu"),
                    getOrEmpty(trip, "shipName"),
                    getOrEmpty(trip, "portName"),
                    "",
                    "",
                    getNestedOrEmpty(visa, "frequency", "valueRu"),
                    getNestedOrEmpty(visa, "category", "valueRu"),
                    getNestedOrEmpty(visa, "type", "valueRu"),
                    getOrEmpty(visa, "number"),
                    getOrEmpty(visa, "beginDate"),
                    getOrEmpty(visa, "endDate"),
                    "",
                    getOrEmpty(doc, "iin"),
                    "",
                    "",
                    ""
            };

            csvLines.add(String.join("|", values));
        }
    }
    // -----------------------------
    // HELPERS
    // -----------------------------
    private String getOrEmpty(JsonNode node, String... path) {
        JsonNode current = node;
        for (String field : path) {
            if (current == null || current.isNull() || current.isMissingNode()) {
                return "";
            }
            current = current.path(field);
        }

        if (current.isNull() || current.isMissingNode()) {
            return "";
        }

        String value = current.asText("");
        return value.equals("null") ? "" : value;
    }

    private String getNestedOrEmpty(JsonNode node, String... path) {
        return getOrEmpty(node, path);
    }

    private String formatName(String given, String surname, String fathers) {
        List<String> parts = new ArrayList<>();

        if (surname != null && !surname.isEmpty() && !surname.equals("null")) {
            parts.add(surname);
        }
        if (given != null && !given.isEmpty() && !given.equals("null")) {
            parts.add(given);
        }
        if (fathers != null && !fathers.isEmpty() && !fathers.equals("null")) {
            parts.add(fathers);
        }

        return parts.isEmpty() ? "" : String.join(" ", parts);
    }

    // -----------------------------
    // COOKIES
    // -----------------------------
    private void extractCookies(ClientResponse resp) {
        resp.headers().header(HttpHeaders.SET_COOKIE).forEach(c -> {
            if (c.contains("SESSION=") || c.contains("INGRESSCOOKIE=")) {
                String clean = c.split(";", 2)[0];
                if (!sessionCookie.contains(clean)) {
                    sessionCookie = sessionCookie.isEmpty() ? clean : sessionCookie + "; " + clean;
                }
            }
        });
    }

    private String extractCode(String loc) {
        int idx = loc.indexOf("code=");
        String part = loc.substring(idx + 5);
        int amp = part.indexOf('&');
        return amp == -1 ? part : part.substring(0, amp);
    }
    public void flushPartialCsv() {
        try {
            if (!exportRunning) {
                System.out.println("No active export, nothing to flush.");
                return;
            }

            List<String> snapshot;

            synchronized (globalCsvLines) {
                snapshot = new ArrayList<>(globalCsvLines);
            }

            if (snapshot.isEmpty()) {
                System.out.println("No CSV data to flush.");
                return;
            }

            Path path = Paths.get("export/shutdown_save.csv");
            Files.createDirectories(path.getParent());
            Files.write(path, snapshot, StandardCharsets.UTF_8);

            System.out.println("✔ Shutdown autosave complete: " + path.toAbsolutePath());

        } catch (Exception e) {
            System.err.println("❌ Failed to flush CSV on shutdown: " + e.getMessage());
        }
    }


    private static void log(String s) {
        System.out.println("[BERKUT] " + s);
    }
}