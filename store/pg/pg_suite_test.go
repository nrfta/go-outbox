package pg

import (
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/neighborly/go-pghelpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	db      *sql.DB
	connStr = getConfig().GenerateAddress()
)

func TestIntegration(t *testing.T) {
	close := setupTestDB(t)
	AfterSuite(close)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Store Integration Suite")
}

var _ = BeforeEach(func() {
	var err error
	cfg := getConfig()
	cfg.Database = "outbox_test"
	db, err = pghelpers.ConnectPostgres(*cfg)
	Expect(err).To(Succeed())
})

var _ = AfterEach(func() {
	db.Exec("DROP TABLE IF EXISTS outbox;")
	db.Close()
})

func setupTestDB(t *testing.T) func() {
	db, err := pghelpers.ConnectPostgres(*getConfig())
	if err != nil {
		t.Fatal(err)
	}

	dropDB := func() {
		_, err := db.Exec("DROP DATABASE IF EXISTS outbox_test;")
		if err != nil {
			t.Fatal(err)
		}
	}
	dropDB()

	_, err = db.Exec("CREATE DATABASE outbox_test;")
	if err != nil {
		t.Fatal(err)
	}

	return dropDB
}

func getConfig() *pghelpers.PostgresConfig {
	var port, _ = strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))
	return &pghelpers.PostgresConfig{
		Host:       getEnv("POSTGRES_HOST", "localhost"),
		Port:       port,
		Username:   getEnv("POSTGRES_USERNAME", "postgres"),
		Password:   getEnv("POSTGRES_PASSWORD", "pgpassword"),
		Database:   getEnv("POSTGRES_DATABASE", "postgres"),
		SSLEnabled: false,
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
