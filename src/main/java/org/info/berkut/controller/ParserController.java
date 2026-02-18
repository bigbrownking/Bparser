package org.info.berkut.controller;

import lombok.RequiredArgsConstructor;
import org.info.berkut.service.Parser;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/parser")
public class ParserController {

    private final Parser parser;

    @GetMapping(value = "/export", produces = "text/csv")
    public Mono<ResponseEntity<String>> exportCsv(
            @RequestParam String dateFrom,
            @RequestParam String dateTo,
            @RequestParam(required = false, defaultValue = "0") int startPage) {

        return parser.exportCsv(dateFrom, dateTo, startPage)
                .map(csv -> ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION,
                                "attachment; filename=\"crossing_facts_" + dateFrom.substring(0,10) + ".csv\"")
                        .contentType(MediaType.parseMediaType("text/csv; charset=UTF-8"))
                        .body(csv)
                );
    }

}
