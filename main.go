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
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {

	ctx := context.Background()

	bucket := os.Getenv("S3_BUCKET")
	key := "798798"

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

	checksumBytes := sha256.Sum256(body)
	checksum := base64.StdEncoding.EncodeToString(checksumBytes[:])

	req, err := PresignPutWithChecksum(ctx, s3c, PutArgs{
		Bucket:         bucket,
		Key:            key,
		ChecksumSha256: checksum,
		ContentLength:  int64(len(body)),
		Expiry:         time.Hour,
	})
	if err != nil {
		log.Fatalln(err)
	}

	r, err := http.NewRequest(req.Method, req.URL, bytes.NewReader(body))
	if err != nil {
		log.Fatalln(err)
	}
	r.Header = req.SignedHeader

	client := &http.Client{
		Transport: &loggingTransport{},
	}
	resp, err := client.Do(r)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("status", resp.Status)
}

type PutArgs struct {
	Bucket         string
	Key            string
	ChecksumSha256 string
	ContentLength  int64
	ContentType    string
	Expiry         time.Duration
}

func PresignPutWithChecksum(
	ctx context.Context,
	s3c *s3.Client,
	args PutArgs,
) (*v4.PresignedHTTPRequest, error) {

	contentType := args.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if args.Expiry == 0 {
		args.Expiry = time.Hour
	}

	// Workaround for: https://github.com/aws/aws-sdk/issues/480
	withPresigner := func(opt *s3.PresignOptions) {
		opt.Presigner = v4.NewSigner(func(so *v4.SignerOptions) {
			// These are the Default settings usually set in s3.newDefaultV4Signer()
			o := s3c.Options()
			so.Logger = o.Logger
			so.LogSigning = o.ClientLogMode.IsSigning()
			so.DisableURIPathEscaping = true

			// This is the magic sauce which makes SHA256 checksums work.
			// It causes the X-Amz-Sdk-Checksum-Algorithm, and X-Amz-Checksum-Sha256 to be included
			// as http headers instead of query parameters in the url. The S3 backend currently
			// silently ignores these if they are sent as query parameters.
			so.DisableHeaderHoisting = true
		})
	}

	psc := s3.NewPresignClient(s3c, withPresigner)

	in := &s3.PutObjectInput{
		Bucket:            aws.String(args.Bucket),
		Key:               aws.String(args.Key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ContentLength:     aws.Int64(args.ContentLength),
		ChecksumSHA256:    aws.String(args.ChecksumSha256),
		ContentType:       aws.String(args.ContentType),
	}

	return psc.PresignPutObject(ctx, in, s3.WithPresignExpires(args.Expiry))
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
