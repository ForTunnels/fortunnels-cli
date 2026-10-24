// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package dataplane

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	// headerDataPlaneFeatures lists the optional data-plane capabilities a
	// server advertises on the data-plane WebSocket upgrade response.
	headerDataPlaneFeatures = "X-Fortunnels-Dataplane-Features"
	// dataPlaneFeatureUDPFlowV1 means the server accepts "framing":"flow" on a
	// reverse UDP stream preface.
	dataPlaneFeatureUDPFlowV1 = "udp-flow-v1"
)

// dataPlaneFeatures are the capabilities read from one data-plane dial.
type dataPlaneFeatures struct {
	udpFlowV1 bool
}

// dataPlaneFeaturesFromResponse reads the advertised features of a dial
// response; a missing response or header means a server without any.
func dataPlaneFeaturesFromResponse(resp *http.Response) dataPlaneFeatures {
	var features dataPlaneFeatures
	if resp == nil {
		return features
	}
	for _, value := range resp.Header.Values(headerDataPlaneFeatures) {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), dataPlaneFeatureUDPFlowV1) {
				features.udpFlowV1 = true
			}
		}
	}
	return features
}

// sendUDPStreamPreface writes the reverse UDP stream preface; flowFraming adds
// "framing":"flow" and must be set only when the server advertised it.
func sendUDPStreamPreface(stream io.Writer, dst, tunnelID string, flowFraming bool) error {
	if !flowFraming {
		return sendUDPPreface(stream, dst, tunnelID)
	}
	payload, err := encodePreface(map[string]string{
		"dst": dst, "proto": "udp", "tunnel_id": tunnelID, "framing": udpFlowFramingName,
	})
	if err != nil {
		return err
	}
	if _, err := stream.Write(payload); err != nil {
		return fmt.Errorf("write preface: %w", err)
	}
	return nil
}
