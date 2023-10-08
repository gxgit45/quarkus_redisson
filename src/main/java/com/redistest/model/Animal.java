package com.redistest.model;


import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@RegisterForReflection
@Getter
@Setter
@EqualsAndHashCode
public class Animal {
    private int id;
    private String aType;
    private int categoryId;
    private String name;
    private String color;
    private String shape;
    private String habitat;
    private String location;
    private String favoriteFood;
}
