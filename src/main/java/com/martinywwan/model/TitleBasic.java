package com.martinywwan.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class TitleBasic implements Serializable {
    private String titleId;
    private String titleType;
    private String primaryTitle;
    private String originalTitle;
    private Boolean isAdult;
    private String startYear;
    private String endYear;
    private int runtimeMinutes;
    private int genres;

    public TitleBasic(String titleId, String titleType, String primaryTitle, String originalTitle, Boolean isAdult, String startYear, String endYear, int runtimeMinutes, int genres) {
        this.titleId = titleId;
        this.titleType = titleType;
        this.primaryTitle = primaryTitle;
        this.originalTitle = originalTitle;
        this.isAdult = isAdult;
        this.startYear = startYear;
        this.endYear = endYear;
        this.runtimeMinutes = runtimeMinutes;
        this.genres = genres;
    }

    @Override
    public String toString() {
        return "TitleBasic{" +
                "titleId='" + titleId + '\'' +
                ", titleType='" + titleType + '\'' +
                ", primaryTitle='" + primaryTitle + '\'' +
                ", originalTitle='" + originalTitle + '\'' +
                ", isAdult=" + isAdult +
                ", startYear='" + startYear + '\'' +
                ", endYear='" + endYear + '\'' +
                ", runtimeMinutes=" + runtimeMinutes +
                ", genres=" + genres +
                '}';
    }
}
