FROM gradle:jdk17
WORKDIR /app
COPY . .
EXPOSE 8081
CMD ["gradle", "run", "--no-daemon"]
