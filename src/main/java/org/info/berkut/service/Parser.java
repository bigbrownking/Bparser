package org.info.berkut.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
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

    private final WebClient webClient;

    // -----------------------------
    // PUBLIC API
    // -----------------------------
    public String exportCsv(String dateFrom, String dateTo) throws Exception {
        String token = authenticateAndGetToken();
        return requestData(dateFrom, dateTo, token);
    }

    // -----------------------------
    // MAIN AUTH FLOW
    // -----------------------------
    private String authenticateAndGetToken() throws JsonProcessingException {
        log("=== STEP 0: GET /auth-service/login ===");
        fetchLoginPage();

        log("=== STEP 1: POST /auth-service/login → get code ===");
        String code = submitLoginForm();

        log("AUTH CODE = " + code);

        log("=== STEP 2: POST /oauth/token → get access_token ===");
        return exchangeCodeForToken(code);
    }

    // -----------------------------
    // GET /auth-service/login
    // -----------------------------
    private void fetchLoginPage() {
        ClientResponse resp = webClient.get()
                .uri(BASE + "/auth-service/login")
                .header(HttpHeaders.USER_AGENT, UA)
                .header(HttpHeaders.ACCEPT, "text/html")
                .exchangeToMono(Mono::just)
                .block();

        String body = readAndLogBody(resp);

        extractCookies(resp);
        log("SESSION COOKIE after GET /login: " + sessionCookie);

        if (!resp.statusCode().is2xxSuccessful()) {
            throw new RuntimeException("GET /login failed: " + resp.statusCode() + " BODY: " + body);
        }
    }

    // -----------------------------
    // POST /auth-service/login → get code
    // -----------------------------
    private String submitLoginForm() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", username);
        form.add("password", password);
        form.add("approve", "on");

        ClientResponse resp = webClient.post()
                .uri(BASE + "/auth-service/login")
                .header(HttpHeaders.COOKIE, sessionCookie)
                .header(HttpHeaders.USER_AGENT, UA)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .exchangeToMono(Mono::just)
                .block();

        readAndLogBody(resp);

        extractCookies(resp);
        log("SESSION after POST /login: " + sessionCookie);

        List<String> loc = resp.headers().header(HttpHeaders.LOCATION);
        if (loc != null && !loc.isEmpty() && loc.get(0).contains("code=")) {
            return extractCode(loc.get(0));
        }

        throw new RuntimeException("Authorization code not found after login!");
    }

    // -----------------------------
    // POST /oauth/token → get access_token
    // -----------------------------
    private String exchangeCodeForToken(String code) throws JsonProcessingException {

        String basic = "Basic " + Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret)
                        .getBytes(StandardCharsets.UTF_8));

        String body = webClient.post()
                .uri(BASE + "/auth-service/oauth/token")
                .header(HttpHeaders.COOKIE, sessionCookie)
                .header(HttpHeaders.AUTHORIZATION, basic)
                .header(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(HttpHeaders.ACCEPT, "application/json")
                .body(BodyInserters.fromFormData("grant_type", "authorization_code")
                        .with("scope", "openid")
                        .with("redirect_uri", redirectUri)
                        .with("code", code)
                )
                .retrieve()
                .bodyToMono(String.class)
                .block();

        Map<String, Object> map =
                mapper.readValue(body, new TypeReference<>() {
                });

        return map.get("access_token").toString();
    }


    // -----------------------------
    // Data Request
    // -----------------------------
    private String requestData(String from, String to, String token) throws JsonProcessingException {
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

        String currentToken = token;
        int currentPage = 0;
        int totalPages = 0;
        List<String> csvLines = new ArrayList<>();
        Random random = new Random();
        boolean headerAdded = false;

        while (true) {
            try {
                // Если это первая итерация или мы начинаем заново после ошибки
                if (currentPage == 0 && !headerAdded) {
                    // Получаем первую страницу для определения общего количества страниц
                    String firstPageBody = webClient.post()
                            .uri(BASE + "/pp-center-service/api/crossing-facts/search?page=0&size=200")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + currentToken)
                            .header(HttpHeaders.CONTENT_TYPE, "application/json")
                            .header(HttpHeaders.COOKIE, sessionCookie)
                            .bodyValue(requestBody)
                            .retrieve()
                            .bodyToMono(String.class)
                            .block();

                    if (firstPageBody == null || firstPageBody.isEmpty()) {
                        return "";
                    }

                    JsonNode firstPageRoot = mapper.readTree(firstPageBody);
                    JsonNode pageInfo = firstPageRoot.path("page");
                    totalPages = pageInfo.path("totalPages").asInt(0);
                    int totalElements = pageInfo.path("totalElements").asInt(0);

                    log("Total records: " + totalElements + ", Total pages: " + totalPages);

                    if (totalPages == 0) {
                        return "";
                    }

                    // Добавляем заголовок один раз
                    String header = "id|taxpayer_name_latin|taxpayer_name_cyrillic|taxpayer_name_original|sex|taxpayer_birthday|citizenship|taxpayer_iin_bin|taxpayer_personal_number|photography_refusing_reason|intersection_status|death_date|death_country|death_reg_place|death_add_information|document_number|document_type|document_issue_date|document_validity_period|document_issue_country|document_issuing_authority|duty_officer_decision|duty_officer_decision_date|duty_officer_decision_add_info|date|detention_place|police_nariad|decision_making_body|decison_making_authority|place_of_birth|location|place_of_work|family_information|education|supression_date|checkpoint|entry_exit_place|trip_purpose|border_crossing_method|direction|exit_country|departure_point|entry_country|destination_pint|system_number|create_date|creater|source|serial_number|status|flight_train_number|belonging|flight_class|flight_type|flight_transport_vessel_number|flight_date_fact|flight_date_plan|vin_code|trailer_number|mark_type|colour|owner|vessel_name|home_port|carriage_quantity|foreign_carriage_quantity|visa_frequency|visa_category|visa_type|visa_number|visa_start_date|visa_expiration_date|document_number1|iin|start_date|expiration_date|actual_date";
                    csvLines.add(header);
                    headerAdded = true;

                    // Обрабатываем первую страницу
                    processPageData(firstPageRoot, csvLines);
                    currentPage = 1;
                }

                // Обрабатываем остальные страницы
                for (int page = currentPage; page < totalPages; page++) {
                    log("Processing page " + (page + 1) + " of " + totalPages);

                    if (page > 0) {
                        long delay = 400 + random.nextLong(500);
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    String pageBody = webClient.post()
                            .uri(BASE + "/pp-center-service/api/crossing-facts/search?page=" + page + "&size=200")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + currentToken)
                            .header(HttpHeaders.CONTENT_TYPE, "application/json")
                            .header(HttpHeaders.COOKIE, sessionCookie)
                            .bodyValue(requestBody)
                            .retrieve()
                            .bodyToMono(String.class)
                            .block();

                    if (pageBody == null || pageBody.isEmpty()) {
                        log("Warning: Page " + page + " returned empty response");
                        continue;
                    }

                    JsonNode root = mapper.readTree(pageBody);
                    processPageData(root, csvLines);

                    currentPage = page + 1; // Сохраняем прогресс
                }

                log("Total records processed: " + (csvLines.size() - 1)); // -1 для исключения заголовка
                return String.join("\n", csvLines);

            } catch (Exception e) {
                String errorMsg = e.getMessage();
                log("Error occurred at page " + currentPage + ": " + errorMsg);

                // Проверяем, является ли это ошибкой сервера (5xx)
                boolean isServerError = errorMsg.contains("502") || errorMsg.contains("503") ||
                        errorMsg.contains("504") || errorMsg.contains("500");

                // Проверяем, является ли это ошибкой авторизации (401, 403)
                boolean isAuthError = errorMsg.contains("401") || errorMsg.contains("403");

                if (isServerError) {
                    // Для ошибок сервера делаем паузу и повторяем попытку
                    log("Server error detected. Waiting 5 seconds before retry...");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    log("Retrying page " + currentPage);
                    // Цикл продолжится с текущей страницы БЕЗ релогина

                } else if (isAuthError) {
                    // Для ошибок авторизации делаем релогин
                    log("Auth error detected. Re-authenticating and continuing from page " + currentPage);
                    try {
                        currentToken = authenticateAndGetToken();
                        log("Successfully re-authenticated. Continuing from page " + currentPage);
                    } catch (Exception authException) {
                        log("Re-authentication failed: " + authException.getMessage());
                        throw new RuntimeException("Failed to re-authenticate: " + authException.getMessage(), authException);
                    }

                } else {
                    // Для других ошибок пробуем релогин
                    log("Unknown error. Re-authenticating and continuing from page " + currentPage);
                    try {
                        currentToken = authenticateAndGetToken();
                        log("Successfully re-authenticated. Continuing from page " + currentPage);
                    } catch (Exception authException) {
                        log("Re-authentication failed: " + authException.getMessage());
                        throw new RuntimeException("Failed to re-authenticate: " + authException.getMessage(), authException);
                    }
                }
            }
        }
    }

    // Вспомогательный метод для обработки данных страницы
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
    // Helper methods

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
    // UTILITIES
    // -----------------------------
    private String readAndLogBody(ClientResponse resp) {
        return resp.bodyToMono(String.class).block();
    }

    private void extractCookies(ClientResponse resp) {
        List<String> cookies = resp.headers().header(HttpHeaders.SET_COOKIE);
        if (cookies == null) return;

        for (String c : cookies) {
            if (c.contains("SESSION=") || c.contains("INGRESSCOOKIE=")) {
                String clean = c.split(";", 2)[0];
                if (!sessionCookie.contains(clean)) {
                    if (sessionCookie.isEmpty()) sessionCookie = clean;
                    else sessionCookie += "; " + clean;
                }
            }
        }
    }

    private String extractCode(String loc) {
        int idx = loc.indexOf("code=");
        String part = loc.substring(idx + 5);
        int amp = part.indexOf('&');
        return amp == -1 ? part : part.substring(0, amp);
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static void log(String s) {
        System.out.println("[BERKUT] " + s);
    }
}
