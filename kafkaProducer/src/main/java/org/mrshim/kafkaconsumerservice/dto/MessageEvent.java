package org.mrshim.kafkaconsumerservice.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MessageEvent {
    private Long id;
    private String message;

}
