package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity @Table(name = "managers")
@Getter @NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Manager {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "auth_sub", nullable = false, unique = true)
    private String authSub;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String organization;

    @Column(unique = true, length = 100)
    private String email;

    @Column(length = 20)
    private String phone;

    @Column(name = "sound_enabled", nullable = false)
    private boolean soundEnabled = true;

    public Manager(String authSub, String name, String organization) {
        this(authSub, name, organization, null);
    }

    public Manager(String authSub, String name, String organization, String phone) {
        this(authSub, name, organization, phone, null);
    }

    public Manager(
            String authSub, String name, String organization, String phone, String email) {
        this.authSub = authSub;
        this.name = name;
        this.organization = organization;
        this.phone = phone;
        this.email = email;
    }

    public void changeSoundSetting(boolean enabled) {
        this.soundEnabled = enabled;
    }

    public void updateProfile(String email, String organization) {
        if (email != null) {
            this.email = email;
        }
        if (organization != null) {
            this.organization = organization;
        }
    }
}
