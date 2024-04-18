package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {

	ctx := context.Background()

	bucket := "<<<<<the-bucket-name-here>>>>"
	key := "<<<<<the-object-key-here>>>>"

	body := []byte(`
And both that morning equally lay
In leaves no step had trodden black.
Oh, I kept the first for another day!
Yet knowing how way leads on to way,
I doubted if I should ever come back.
`)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	s3c := s3.NewFromConfig(cfg)

	req, err := PresignPutWithSha256(ctx, s3c, bucket, key, body)
	if err != nil {
		log.Fatalln(err)
	}

	r, err := http.NewRequest(req.Method, req.URL, bytes.NewReader(body))
	if err != nil {
		log.Fatalln(err)
	}
	r.Header = req.SignedHeader

	client := &http.Client{
		// Uncomment to see all of the things.
		// Transport: &loggingTransport{},
	}
	resp, err := client.Do(r)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("status", resp.Status)
}

func PresignPutWithSha256(
	ctx context.Context,
	s3c *s3.Client,
	bucket, key string,
	body []byte,
) (*v4.PresignedHTTPRequest, error) {

	checksumBytes := sha256.Sum256(body)
	checksum := base64.StdEncoding.EncodeToString(checksumBytes[:])

	withPresigner := func(opt *s3.PresignOptions) {
		opt.Presigner = v4.NewSigner(func(so *v4.SignerOptions) {
			// I copied the Default settings.
			o := s3c.Options()
			so.Logger = o.Logger
			so.LogSigning = o.ClientLogMode.IsSigning()
			so.DisableURIPathEscaping = true

			// This is the magic sauce which makes SHA256 checksums work.
			so.DisableHeaderHoisting = true
		})
	}

	psc := s3.NewPresignClient(s3c)

	in := &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(checksum),
		// Try it with an incorrect checksum to make sure it fails.
		//ChecksumSHA256: aws.String("bad+nDnO0vTnour8G+5YPPx4tLMvxgS0K3ZBusogZxU="),
		ContentLength: aws.Int64(int64(len(body))),
	}

	return psc.PresignPutObject(ctx, in, s3.WithPresignExpires(time.Hour), withPresigner)
}

type loggingTransport struct{}

func (s *loggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	bytes, _ := httputil.DumpRequestOut(r, true)

	resp, err := http.DefaultTransport.RoundTrip(r)
	// err is returned after dumping the response

	respBytes, _ := httputil.DumpResponse(resp, true)
	bytes = append(bytes, respBytes...)

	fmt.Printf("%s\n", bytes)

	return resp, err
}
