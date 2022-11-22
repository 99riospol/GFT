package org.example;

import java.io.Serializable;
import java.sql.Timestamp;

public class Message implements Serializable {
    private String quote;
    private String character;
    private String image;
    private String characterDirection;
    private Timestamp timestamp;

    public Message() {
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getCharacterDirection() {
        return characterDirection;
    }

    public void setCharacterDirection(String characterDirection) {
        this.characterDirection = characterDirection;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
