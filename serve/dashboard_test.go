package serve

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDashboardShellIsPublicOnlyBehindTLS(t *testing.T) {
	material, _, _, err := newX6Material()
	if err != nil {
		t.Fatal(err)
	}
	dependencies := newX6State().dependencies()
	dependencies.Dashboard = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("dashboard shell"))
	})
	server, err := New(t.Context(), x6Config(), material, dependencies)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := httptest.NewRequest(http.MethodGet, "http://fixture.invalid/dashboard/", nil)
	plaintextResponse := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(plaintextResponse, plaintext)
	if plaintextResponse.Code != http.StatusUpgradeRequired {
		t.Fatalf("plaintext dashboard status = %d", plaintextResponse.Code)
	}

	dashboard := httptest.NewRequest(http.MethodGet, "https://fixture.invalid/dashboard/", nil)
	dashboard.TLS = &tls.ConnectionState{}
	dashboardResponse := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(dashboardResponse, dashboard)
	if dashboardResponse.Code != http.StatusOK || dashboardResponse.Body.String() != "dashboard shell" {
		t.Fatalf("TLS dashboard response = %d %q", dashboardResponse.Code, dashboardResponse.Body.String())
	}

	api := httptest.NewRequest(http.MethodGet, "https://fixture.invalid/v1/catalog/sources", nil)
	api.TLS = &tls.ConnectionState{}
	apiResponse := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(apiResponse, api)
	if apiResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated data API status = %d", apiResponse.Code)
	}
}
