package org.onedatashare.transferservice.odstransferservice.service;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.vault.core.VaultPkiOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultIssuerCertificateRequestResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Enumeration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class VaultSSLService {

    private final VaultPkiOperations vaultPkiOperations;
    @Getter
    private final Path storePath;
    private final Logger logger = LoggerFactory.getLogger(VaultSSLService.class);
    private final String keyStorePassword;
    private final ScheduledExecutorService scheduler;

    @Getter
    public Duration storeDuration;

    public VaultSSLService(Environment environment, VaultTemplate vaultTemplate) {
        this.vaultPkiOperations = vaultTemplate.opsForPki();
        this.storePath = Paths.get(System.getProperty("user.home"), "onedatashare", "ftn", "store", "jobscheduler.truststore.p12");
        this.keyStorePassword = environment.getProperty("hz.keystore.password", "changeit");
        this.storeDuration = Duration.ofDays(1);
        this.scheduler = Executors.newScheduledThreadPool(0, Thread.ofVirtual().factory());

    }

    @PostConstruct
    public void init() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                logger.info("Running Certificate CRON");
                refreshCerts();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }, 0, Duration.ofMinutes(1).toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
    }

    public void refreshCerts() throws KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        logger.info("Refreshing Certificates");
        KeyStore trustStore = this.readInTrustStore();
        boolean hasValidCerts = this.checkIfCertsAreStillValid(trustStore);
        logger.info("Certs are valid: {}", hasValidCerts);
        if (trustStore == null || !hasValidCerts) {
            VaultIssuerCertificateRequestResponse resp = this.vaultPkiOperations.getIssuerCertificate("7022f845-246c-3836-836f-83936e50b888");
            trustStore = resp.getData().createTrustStore(true);
            this.persistStore(trustStore);
        }
    }


    private KeyStore readInTrustStore() throws KeyStoreException {
        if (Files.exists(storePath)) {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (InputStream inputStream = Files.newInputStream(storePath, StandardOpenOption.READ)) {
                keyStore.load(inputStream, this.keyStorePassword.toCharArray());
                return keyStore;
            } catch (IOException | CertificateException | NoSuchAlgorithmException e) {
                return null;
            }
        }
        return null;
    }

    private boolean checkIfCertsAreStillValid(KeyStore keyStore) throws KeyStoreException {
        if (keyStore == null) return false;
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            X509Certificate certificate = (X509Certificate) keyStore.getCertificate(alias);
            try {
                certificate.checkValidity();
            } catch (CertificateExpiredException | CertificateNotYetValidException e) {
                return false;
            }
        }
        return true;
    }

    private void persistStore(KeyStore store) throws IOException, KeyStoreException, NoSuchAlgorithmException {
        if (!Files.exists(storePath)) {
            Files.createDirectories(storePath.getParent());
            Files.createFile(storePath);
        }
        try (OutputStream outputStream = Files.newOutputStream(storePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            logger.debug("Persisting the KeyStore to {}", storePath);
            try {
                store.store(outputStream, this.keyStorePassword.toCharArray());
            } catch (CertificateException e) {
            }
        }
    }
}
