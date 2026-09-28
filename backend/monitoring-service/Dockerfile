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
# 한글 로그가 docker logs에서 ???로 깨지지 않게 표준 출력 인코딩을 고정한다.
# alpine에는 로케일이 없어 JDK 21이 콘솔 인코딩을 ASCII로 잡는다.
ENTRYPOINT ["java", "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-jar", "app.jar"]
