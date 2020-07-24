FROM centos:7
WORKDIR /traefik
#COPY defaultProtos /traefik/defaultProtos
COPY traefik.toml /traefik/traefik.toml
COPY conf /traefik/conf
COPY traefik /traefik/traefik
HEALTHCHECK --interval=5s --timeout=2s --retries=3  CMD curl -f http://127.0.0.1:8079/ping || exit 1


#eg: h2c://127.0.0.1:8081
ENV BACKENDS ""
ENV LOG_LEVEL "WARN"
EXPOSE 8080

CMD ./traefik --configFile traefik.toml