package com.martinywwan.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class NameBasic implements Serializable {
    private String nameId;
    private String primaryName;
    private String birthYear;
    private String deathYear;
    private String primaryProfession;
    private String knownForTitles;

    public NameBasic(String nameId, String primaryName, String birthYear, String deathYear, String primaryProfession, String knownForTitles) {
        this.nameId = nameId;
        this.primaryName = primaryName;
        this.birthYear = birthYear;
        this.deathYear = deathYear;
        this.primaryProfession = primaryProfession;
        this.knownForTitles = knownForTitles;
    }
    @Override
    public String toString() {
        return "NameBasic{" +
                "nameId='" + nameId + '\'' +
                ", primaryName='" + primaryName + '\'' +
                ", birthYear='" + birthYear + '\'' +
                ", deathYear='" + deathYear + '\'' +
                ", primaryProfession='" + primaryProfession + '\'' +
                ", knownForTitles='" + knownForTitles + '\'' +
                '}';
    }
}
