package org.info.berkut.controller;


import lombok.RequiredArgsConstructor;
import org.info.berkut.service.Parser;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/parser")
public class ParserController {

    private final Parser parser;

    @GetMapping(value = "/export", produces = "text/csv")
    public ResponseEntity<String> exportCsv(
            @RequestParam String dateFrom,
            @RequestParam String dateTo) throws Exception {

        String csv = parser.exportCsv(dateFrom, dateTo);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"crossing_facts_" + dateFrom.substring(0,10) + ".csv\"")
                .header(HttpHeaders.CONTENT_TYPE, "text/csv; charset=UTF-8")
                .body(csv);
    }

}
