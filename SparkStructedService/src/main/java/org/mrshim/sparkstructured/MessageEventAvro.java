package org.mrshim.sparkstructured;


import lombok.ToString;

@ToString
public class MessageEventAvro {
    private long id;
    private String message;

    public MessageEventAvro() {
        // Пустой конструктор, необходимый для десериализации
    }

    public MessageEventAvro(long id, String message) {
        this.id = id;
        this.message = message;
    }

    // Геттеры и сеттеры для полей id и message

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
