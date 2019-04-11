package fargo

// MIT Licensed (see README.md) - Copyright (c) 2013 Hudl <@Hudl>

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/miekg/dns"
)

func discoverDNS(region string, domain string, port int, urlBase string) (servers []string, ttl time.Duration, err error) {
	// all DNS queries must use the FQDN
	domain = "txt." + region + "." + dns.Fqdn(domain)
	log.Debugf("domain: %s", domain)
	if _, ok := dns.IsDomainName(domain); !ok {
		err = fmt.Errorf("invalid domain name: '%s' is not a domain name", domain)
		return
	}
	regionRecords, ttl, err := retryingFindTXT(domain)
	log.Debugf("regionRecords: %v", regionRecords)
	if err != nil {
		return
	}

	for _, az := range regionRecords {
		instances, _, er := retryingFindTXT("txt." + dns.Fqdn(az))
		if er != nil {
			continue
		}
		for _, instance := range instances {
			// format the service URL
			servers = append(servers, fmt.Sprintf("http://%s:%d/%s", instance, port, urlBase))
		}
	}
	log.Debugf("servers: %v", servers)
	return
}

// retryingFindTXT will, on any DNS failure, retry for up to 15 minutes before
// giving up and returning an empty []string of records
func retryingFindTXT(fqdn string) (records []string, ttl time.Duration, err error) {
	err = backoff.Retry(
		func() error {
			records, ttl, err = findTXT(fqdn)
			if err != nil {
				log.Errorf("Retrying DNS query. Query failed with: %s", err.Error())
			}
			return err
		}, backoff.NewExponentialBackOff())
	return
}

func findTXT(fqdn string) ([]string, time.Duration, error) {
	defaultTTL := 120 * time.Second
	query := new(dns.Msg)
	query.SetQuestion(fqdn, dns.TypeTXT)
	dnsServerAddr, err := findDnsServerAddr()
	if err != nil {
		log.Errorf("Failure finding DNS server, err=%s", err.Error())
		return nil, defaultTTL, err
	}

	response, err := dns.Exchange(query, dnsServerAddr)
	if err != nil {
		log.Errorf("Failure resolving name %s err=%s", fqdn, err.Error())
		return nil, defaultTTL, err
	}
	if len(response.Answer) < 1 {
		err := fmt.Errorf("no Eureka discovery TXT record returned for name=%s", fqdn)
		log.Errorf("no answer for name=%s err=%s", fqdn, err.Error())
		return nil, defaultTTL, err
	}
	if response.Answer[0].Header().Rrtype != dns.TypeTXT {
		err := fmt.Errorf("did not receive TXT record back from query specifying TXT record. This should never happen.")
		log.Errorf("Failure resolving name %s err=%s", fqdn, err.Error())
		return nil, defaultTTL, err
	}
	txt := response.Answer[0].(*dns.TXT)
	ttl := response.Answer[0].Header().Ttl
	if ttl < 60 {
		ttl = 60
	}

	return txt.Txt, time.Duration(ttl) * time.Second, nil
}

func findDnsServerAddr() (string, error) {
	// Find a DNS server using the OS resolv.conf
	config, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		log.Errorf("Failure finding DNS server address from /etc/resolv.conf, err = %s", err)
		return "", err
	} else {
		return config.Servers[0] + ":" + config.Port, nil
	}
}
