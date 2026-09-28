# 1단계: 빌드 (JDK + Gradle wrapper로 jar 생성)
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew && ./gradlew test bootJar --no-daemon

# 2단계: 실행 (가벼운 JRE만, 빌드 도구 없음)
FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
USER app
ENTRYPOINT ["java", "-jar", "app.jar"]
