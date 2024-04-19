package eigenda

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenda/api/grpc/disperser"
	"github.com/Layr-Labs/eigenda/encoding/utils/codec"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type EigenDA struct {
	Config

	Log log.Logger
}

type EigenDAMessage struct {
	BlobHeader []byte
	BlobIndex  uint32
}

func (m EigenDA) Init() error {
	return nil
}

func (m EigenDA) GetSequence(ctx context.Context, batchHashes []common.Hash, dataAvailabilityMessage []byte) ([][]byte, error) {
	println("GetSequence")
	// TODO - load commitments from daMessage

	println("RLP decoding dataAvailabilityMessage", fmt.Sprintf("%08b", dataAvailabilityMessage))
	// 1.a - RLP decode message
	var msg EigenDAMessage
	if err := rlp.DecodeBytes(dataAvailabilityMessage, &msg); err != nil {
		return nil, err
	}
	println("Decoded dataAvailabilityMessage", fmt.Sprintf("%+v", msg))
	// 1.b - Unmarshal decoded RLP bytes to EigenDA commit message

	println("Retrieving blob from EigenDA")
	// 2 - Establish GRPC connection with disperser node
	config := &tls.Config{}
	credential := credentials.NewTLS(config)
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(credential)}
	conn, err := grpc.Dial(m.RPC, dialOptions...)
	if err != nil {
		return nil, err
	}
	daClient := disperser.NewDisperserClient(conn)

	request := &disperser.RetrieveBlobRequest{
		BatchHeaderHash: msg.BlobHeader,
		// TODO - figure out how to bind this
		BlobIndex: msg.BlobIndex,
	}

	println("Retrieving blob from EigenDA", fmt.Sprintf("%+v", request))

	reply, err := daClient.RetrieveBlob(ctx, request)
	if err != nil {
		return nil, err
	}

	println("Blob retrieved from EigenDA")
	// decode modulo bn254
	println("Decoding blob data")
	decodedData := codec.RemoveEmptyByteFromPaddedBytes(reply.Data)

	println("Decoded blob data", decodedData)
	// rlp decode to 2D batch array
	var batches [][]byte
	if err := rlp.DecodeBytes(decodedData, &batches); err != nil {
		return nil, err
	}

	println("Successfully retrieved sequence from EigenDA")
	println("batches", batches)
	return batches, nil
}

func (m EigenDA) PostSequence(ctx context.Context, batchesData [][]byte) ([]byte, error) {
	println("Attempting to post sequence to EigenDA")
	config := &tls.Config{}
	credential := credentials.NewTLS(config)
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(credential)}
	println("Dialing EigenDA at", m.RPC)
	conn, err := grpc.Dial(m.RPC, dialOptions...)
	if err != nil {
		return nil, err
	}

	println("batches", batchesData)
	println("Constructing disperser client")
	daClient := disperser.NewDisperserClient(conn)

	/*
		Map N batches to 1 one blob
	*/

	println("Encoding batches to RLP")
	encodedBytes, err := rlp.EncodeToBytes(batchesData)
	if err != nil {
		return nil, err
	}

	println("Encoding for bn254 curve compatibility")
	// encode modulo bn254
	encodedTxData := codec.ConvertByPaddingEmptyByte(encodedBytes)

	disperseReq := &disperser.DisperseBlobRequest{
		Data: encodedTxData,
	}

	println("Dispersing blob to EigenDA")
	disperseRes, err := daClient.DisperseBlob(ctx, disperseReq)

	if err != nil || disperseRes == nil {
		log.Error("Unable to disperse blob to EigenDA, aborting", "err", err)
		return nil, err
	}
	println("DisperseBlob response", disperseRes)

	if disperseRes.Result == disperser.BlobStatus_UNKNOWN ||
		disperseRes.Result == disperser.BlobStatus_FAILED {
		log.Error("Unable to disperse blob to EigenDA, aborting", "err", err)
		return nil, fmt.Errorf("reply status is %d", disperseRes.Result)
	}

	base64RequestID := base64.StdEncoding.EncodeToString(disperseRes.RequestId)

	println("Blob dispersed to EigenDA, now waiting for confirmation", "requestID", base64RequestID)

	var statusRes *disperser.BlobStatusReply
	timeoutTime := time.Now().Add(m.StatusQueryTimeout)
	// Wait before first status check
	time.Sleep(m.StatusQueryRetryInterval)
	for time.Now().Before(timeoutTime) {
		statusRes, err = daClient.GetBlobStatus(ctx, &disperser.BlobStatusRequest{
			RequestId: disperseRes.RequestId,
		})
		if err != nil {
			println("Unable to retrieve blob dispersal status, will retry", "requestID", base64RequestID, "err", err)
		} else if statusRes.Status == disperser.BlobStatus_CONFIRMED || statusRes.Status == disperser.BlobStatus_FINALIZED {
			batchHeaderHashHex := fmt.Sprintf("0x%s", hex.EncodeToString(statusRes.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash))
			println("Successfully dispersed blob to EigenDA", "requestID", base64RequestID, "batchHeaderHash", batchHeaderHashHex)

			msg, err := rlp.EncodeToBytes(EigenDAMessage{
				BlobHeader: statusRes.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash,
				BlobIndex:  statusRes.Info.BlobVerificationProof.BlobIndex,
			})

			if err != nil {
				log.Error("Unable to encode EigenDA message", "err", err)
				return nil, err
			}

			println("Returning EigenDA message", fmt.Sprintf("%08b", msg))
			return msg, nil

		} else if statusRes.Status == disperser.BlobStatus_UNKNOWN ||
			statusRes.Status == disperser.BlobStatus_FAILED {
			log.Error("EigenDA blob dispersal failed in processing", "requestID", base64RequestID, "err", err)
			return nil, fmt.Errorf("eigenDA blob dispersal failed in processing with reply status %d", statusRes.Status)
		} else {
			println("Still waiting for confirmation from EigenDA", "requestID", base64RequestID)
		}

		// Wait before first status check
		time.Sleep(m.StatusQueryRetryInterval)
	}
	return nil, fmt.Errorf("timed out getting EigenDA status for dispersed blob key: %s", base64RequestID)

}
