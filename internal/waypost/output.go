package waypost

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

func writeNextCursor(w io.Writer, cursor string) error {
	if cursor == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "next_cursor=%s\n", cursor)
	return err
}

type outputFormat uint8

const (
	outputFormatText outputFormat = iota
	outputFormatJSON
	outputFormatYAML
	outputFormatNDJSON
)

type outputFlags struct {
	json   bool
	yaml   bool
	ndjson bool
}

func (f *outputFlags) register(fs *flag.FlagSet, jsonUsage, yamlUsage string) {
	fs.BoolVar(&f.json, "json", false, jsonUsage)
	fs.BoolVar(&f.yaml, "yaml", false, yamlUsage)
	fs.BoolVar(&f.ndjson, "ndjson", false, "emit newline-delimited JSON")
}

func (f outputFlags) resolve() (outputFormat, error) {
	if (f.json && f.yaml) || (f.json && f.ndjson) || (f.yaml && f.ndjson) {
		return outputFormatText, errors.New("--json, --ndjson, and --yaml are mutually exclusive")
	}
	if f.yaml {
		return outputFormatYAML, nil
	}
	if f.json {
		return outputFormatJSON, nil
	}
	if f.ndjson {
		return outputFormatNDJSON, nil
	}
	return outputFormatText, nil
}

func (f outputFlags) resolveStructured() (outputFormat, error) {
	format, err := f.resolve()
	if err != nil {
		return outputFormatText, err
	}
	if format == outputFormatText {
		return outputFormatText, errors.New("either --json, --ndjson, or --yaml is required")
	}
	return format, nil
}

func (a *App) writeStructuredOutput(format outputFormat, value any) error {
	switch format {
	case outputFormatJSON:
		encoder := json.NewEncoder(a.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	case outputFormatNDJSON:
		return json.NewEncoder(a.stdout).Encode(value)
	case outputFormatYAML:
		return writeYAML(a.stdout, value)
	default:
		return fmt.Errorf("unsupported structured output format %d", format)
	}
}

func (a *App) writeSendResultText(result SendResultCompact) error {
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		_, err := fmt.Fprintf(
			a.stdout,
			"message_id=%s group=%s eligible_count=%d\n",
			result.MessageID,
			result.GroupAddress,
			eligibleCount,
		)
		return err
	}
	_, err := fmt.Fprintf(a.stdout, "delivery_id=%s\n", result.DeliveryID)
	return err
}

func (a *App) writeSendResultFullText(result SendResultFull) error {
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		_, err := fmt.Fprintf(
			a.stdout,
			"message_id=%s group=%s eligible_count=%d\n",
			result.MessageID,
			result.GroupAddress,
			eligibleCount,
		)
		return err
	}
	_, err := fmt.Fprintf(
		a.stdout,
		"message_id=%s delivery_id=%s blob_id=%s\n",
		result.MessageID,
		result.DeliveryID,
		result.BlobID,
	)
	return err
}

func formatSendNotificationText(status string, scheme, notifyDetail, notifyError *string) string {
	text := fmt.Sprintf("notify_status=%s", status)
	if scheme != nil {
		text += fmt.Sprintf(" notify_scheme=%s", *scheme)
	}
	if notifyDetail != nil {
		text += fmt.Sprintf(" notify_detail=%q", *notifyDetail)
	}
	if notifyError != nil {
		text += fmt.Sprintf(" notify_error=%q", *notifyError)
	}
	return text
}

func (a *App) writeSendResultTextWithNotification(result SendResultCompactWithNotification) error {
	line := ""
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		line = fmt.Sprintf("message_id=%s group=%s eligible_count=%d", result.MessageID, result.GroupAddress, eligibleCount)
	} else {
		line = fmt.Sprintf("delivery_id=%s", result.DeliveryID)
	}
	_, err := fmt.Fprintf(a.stdout, "%s %s\n", line, formatSendNotificationText(result.NotifyStatus, result.NotifyScheme, result.NotifyDetail, result.NotifyError))
	return err
}

func (a *App) writeSendResultFullTextWithNotification(result SendResultFullWithNotification) error {
	line := ""
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		line = fmt.Sprintf("message_id=%s group=%s eligible_count=%d", result.MessageID, result.GroupAddress, eligibleCount)
	} else {
		line = fmt.Sprintf("message_id=%s delivery_id=%s blob_id=%s", result.MessageID, result.DeliveryID, result.BlobID)
	}
	_, err := fmt.Fprintf(a.stdout, "%s %s\n", line, formatSendNotificationText(result.NotifyStatus, result.NotifyScheme, result.NotifyDetail, result.NotifyError))
	return err
}

func (a *App) writeSendBatchText(result SendBatchOutput, full bool) error {
	for _, item := range result.Results {
		if _, err := fmt.Fprintln(a.stdout, formatSendBatchItemText(item, full)); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(
		a.stdout,
		"status=%s recipient_count=%d sent_count=%d failed_count=%d\n",
		result.Status,
		result.RecipientCount,
		result.SentCount,
		result.FailedCount,
	)
	return err
}

func formatSendBatchItemText(item SendBatchItemOutput, full bool) string {
	prefix := fmt.Sprintf("to_address=%s status=%s", item.ToAddress, item.Status)
	if item.Status == "failed" {
		line := fmt.Sprintf("%s error=%q", prefix, item.Error)
		if item.NotifyStatus != nil {
			line += " " + formatSendNotificationText(*item.NotifyStatus, item.NotifyScheme, item.NotifyDetail, item.NotifyError)
		}
		return line
	}

	line := prefix
	if item.Mode == SendModeGroup {
		eligibleCount := 0
		if item.EligibleCount != nil {
			eligibleCount = *item.EligibleCount
		}
		line += fmt.Sprintf(" message_id=%s group=%s eligible_count=%d", item.MessageID, item.GroupAddress, eligibleCount)
	} else if full {
		line += fmt.Sprintf(" message_id=%s delivery_id=%s blob_id=%s", item.MessageID, item.DeliveryID, item.BlobID)
	} else {
		line += fmt.Sprintf(" delivery_id=%s", item.DeliveryID)
	}
	if item.NotifyStatus != nil {
		line += " " + formatSendNotificationText(*item.NotifyStatus, item.NotifyScheme, item.NotifyDetail, item.NotifyError)
	}
	return line
}

func (a *App) writeForwardResultText(result ForwardResultCompact) error {
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		if result.SourceDeliveryID != "" {
			_, err := fmt.Fprintf(
				a.stdout,
				"message_id=%s group=%s eligible_count=%d source_message_id=%s source_delivery_id=%s\n",
				result.MessageID,
				result.GroupAddress,
				eligibleCount,
				result.SourceMessageID,
				result.SourceDeliveryID,
			)
			return err
		}
		_, err := fmt.Fprintf(
			a.stdout,
			"message_id=%s group=%s eligible_count=%d source_message_id=%s\n",
			result.MessageID,
			result.GroupAddress,
			eligibleCount,
			result.SourceMessageID,
		)
		return err
	}
	if result.SourceDeliveryID != "" {
		_, err := fmt.Fprintf(
			a.stdout,
			"delivery_id=%s source_message_id=%s source_delivery_id=%s\n",
			result.DeliveryID,
			result.SourceMessageID,
			result.SourceDeliveryID,
		)
		return err
	}
	_, err := fmt.Fprintf(a.stdout, "delivery_id=%s source_message_id=%s\n", result.DeliveryID, result.SourceMessageID)
	return err
}

func (a *App) writeForwardResultFullText(result ForwardResultFull) error {
	if result.Mode == SendModeGroup {
		eligibleCount := 0
		if result.EligibleCount != nil {
			eligibleCount = *result.EligibleCount
		}
		if result.SourceDeliveryID != "" {
			_, err := fmt.Fprintf(
				a.stdout,
				"message_id=%s group=%s eligible_count=%d source_message_id=%s source_delivery_id=%s\n",
				result.MessageID,
				result.GroupAddress,
				eligibleCount,
				result.SourceMessageID,
				result.SourceDeliveryID,
			)
			return err
		}
		_, err := fmt.Fprintf(
			a.stdout,
			"message_id=%s group=%s eligible_count=%d source_message_id=%s\n",
			result.MessageID,
			result.GroupAddress,
			eligibleCount,
			result.SourceMessageID,
		)
		return err
	}
	if result.SourceDeliveryID != "" {
		_, err := fmt.Fprintf(
			a.stdout,
			"message_id=%s delivery_id=%s blob_id=%s source_message_id=%s source_delivery_id=%s\n",
			result.MessageID,
			result.DeliveryID,
			result.BlobID,
			result.SourceMessageID,
			result.SourceDeliveryID,
		)
		return err
	}
	_, err := fmt.Fprintf(
		a.stdout,
		"message_id=%s delivery_id=%s blob_id=%s source_message_id=%s\n",
		result.MessageID,
		result.DeliveryID,
		result.BlobID,
		result.SourceMessageID,
	)
	return err
}

func (a *App) writeReceivedMessageText(message ReceivedMessageCompact) error {
	header := fmt.Sprintf(
		"delivery_id=%s recipient_address=%s lease_token=%s content_type=%s subject=%q",
		message.DeliveryID,
		message.RecipientAddress,
		message.LeaseToken,
		message.ContentType,
		message.Subject,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	if _, err := fmt.Fprintln(a.stdout, header); err != nil {
		return err
	}
	if _, err := fmt.Fprint(a.stdout, message.Body); err != nil {
		return err
	}
	if !strings.HasSuffix(message.Body, "\n") {
		if _, err := fmt.Fprintln(a.stdout); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReceivedMessageFullText(message ReceivedMessage) error {
	header := fmt.Sprintf(
		"delivery_id=%s message_id=%s recipient_address=%s lease_token=%s lease_expires_at=%s subject=%q",
		message.DeliveryID,
		message.MessageID,
		message.RecipientAddress,
		message.LeaseToken,
		message.LeaseExpiresAt,
		message.Subject,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	if _, err := fmt.Fprintln(a.stdout, header); err != nil {
		return err
	}
	if _, err := fmt.Fprint(a.stdout, message.Body); err != nil {
		return err
	}
	if !strings.HasSuffix(message.Body, "\n") {
		if _, err := fmt.Fprintln(a.stdout); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeGroupReceivedMessageText(message GroupReceivedMessageCompact) error {
	header := fmt.Sprintf(
		"message_id=%s group=%s person=%s first_read_at=%s content_type=%s subject=%q read_count=%d eligible_count=%d",
		message.MessageID,
		message.GroupAddress,
		message.Person,
		message.FirstReadAt,
		message.ContentType,
		message.Subject,
		message.ReadCount,
		message.EligibleCount,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	if _, err := fmt.Fprintln(a.stdout, header); err != nil {
		return err
	}
	if _, err := fmt.Fprint(a.stdout, message.Body); err != nil {
		return err
	}
	if !strings.HasSuffix(message.Body, "\n") {
		if _, err := fmt.Fprintln(a.stdout); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReceiveResultText(result ReceiveResultCompact) error {
	for index, message := range result.Messages {
		if index > 0 {
			if _, err := io.WriteString(a.stdout, "---\n"); err != nil {
				return err
			}
		}
		if err := a.writeReceivedMessageText(message); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReceiveResultFullText(result ReceiveResult) error {
	for index, message := range result.Messages {
		if index > 0 {
			if _, err := io.WriteString(a.stdout, "---\n"); err != nil {
				return err
			}
		}
		if err := a.writeReceivedMessageFullText(message); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeDeliveryTransitionResultText(result DeliveryTransitionResult) error {
	switch {
	case result.AckedAt != "":
		_, err := fmt.Fprintf(a.stdout, "delivery_id=%s state=%s acked_at=%s attempt_count=%d\n", result.DeliveryID, result.State, result.AckedAt, result.AttemptCount)
		return err
	case result.VisibleAt != "":
		_, err := fmt.Fprintf(a.stdout, "delivery_id=%s state=%s visible_at=%s attempt_count=%d\n", result.DeliveryID, result.State, result.VisibleAt, result.AttemptCount)
		return err
	default:
		_, err := fmt.Fprintf(a.stdout, "delivery_id=%s state=%s attempt_count=%d\n", result.DeliveryID, result.State, result.AttemptCount)
		return err
	}
}

func (a *App) writeLeaseRenewResultText(result LeaseRenewResult) error {
	_, err := fmt.Fprintf(a.stdout, "delivery_id=%s lease_token=%s lease_expires_at=%s\n", result.DeliveryID, result.LeaseToken, result.LeaseExpiresAt)
	return err
}

func (a *App) writeReadDeliveryText(delivery ReadDelivery) error {
	header := ""
	if delivery.AckedAt != nil {
		header = fmt.Sprintf(
			"delivery_id=%s recipient_address=%s state=%s visible_at=%s acked_at=%s content_type=%s subject=%q",
			delivery.DeliveryID,
			delivery.RecipientAddress,
			delivery.State,
			delivery.VisibleAt,
			*delivery.AckedAt,
			delivery.ContentType,
			delivery.Subject,
		)
	} else {
		header = fmt.Sprintf(
			"delivery_id=%s recipient_address=%s state=%s visible_at=%s content_type=%s subject=%q",
			delivery.DeliveryID,
			delivery.RecipientAddress,
			delivery.State,
			delivery.VisibleAt,
			delivery.ContentType,
			delivery.Subject,
		)
	}
	if delivery.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *delivery.ForwardedFromAddress)
	}
	if delivery.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *delivery.SenderAddress)
	}
	if _, err := fmt.Fprintln(a.stdout, header); err != nil {
		return err
	}
	if _, err := fmt.Fprint(a.stdout, delivery.Body); err != nil {
		return err
	}
	if !strings.HasSuffix(delivery.Body, "\n") {
		if _, err := fmt.Fprintln(a.stdout); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReadMessageText(message ReadMessage) error {
	header := fmt.Sprintf(
		"message_id=%s content_type=%s subject=%q",
		message.MessageID,
		message.ContentType,
		message.Subject,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	if _, err := fmt.Fprintln(a.stdout, header); err != nil {
		return err
	}
	if _, err := fmt.Fprint(a.stdout, message.Body); err != nil {
		return err
	}
	if !strings.HasSuffix(message.Body, "\n") {
		if _, err := fmt.Fprintln(a.stdout); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReadMessageResultText(result readMessageResult) error {
	for index, message := range result.Items {
		if index > 0 {
			if _, err := io.WriteString(a.stdout, "---\n"); err != nil {
				return err
			}
		}
		if err := a.writeReadMessageText(message); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeReadDeliveryResultText(result readDeliveryResult) error {
	for index, delivery := range result.Items {
		if index > 0 {
			if _, err := io.WriteString(a.stdout, "---\n"); err != nil {
				return err
			}
		}
		if err := a.writeReadDeliveryText(delivery); err != nil {
			return err
		}
	}
	return writeNextCursor(a.stdout, result.NextCursor)
}

func (a *App) writeListedDeliveryText(delivery ListedDelivery) error {
	forwardedFrom := ""
	if delivery.ForwardedFromAddress != nil {
		forwardedFrom = fmt.Sprintf(" forwarded_from_address=%s", *delivery.ForwardedFromAddress)
	}
	if delivery.SenderAddress != nil {
		forwardedFrom += fmt.Sprintf(" sender_address=%s", *delivery.SenderAddress)
	}
	if delivery.AckedAt != nil {
		_, err := fmt.Fprintf(
			a.stdout,
			"delivery_id=%s recipient_address=%s state=%s visible_at=%s acked_at=%s subject=%q%s\n",
			delivery.DeliveryID,
			delivery.RecipientAddress,
			delivery.State,
			delivery.VisibleAt,
			*delivery.AckedAt,
			delivery.Subject,
			forwardedFrom,
		)
		return err
	}
	_, err := fmt.Fprintf(
		a.stdout,
		"delivery_id=%s recipient_address=%s state=%s visible_at=%s subject=%q%s\n",
		delivery.DeliveryID,
		delivery.RecipientAddress,
		delivery.State,
		delivery.VisibleAt,
		delivery.Subject,
		forwardedFrom,
	)
	return err
}

func (a *App) writeWaitedDeliveryText(delivery ListedDeliveryCompact) error {
	header := fmt.Sprintf(
		"delivery_id=%s recipient_address=%s content_type=%s subject=%q",
		delivery.DeliveryID,
		delivery.RecipientAddress,
		delivery.ContentType,
		delivery.Subject,
	)
	if delivery.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *delivery.ForwardedFromAddress)
	}
	_, err := fmt.Fprintln(a.stdout, header)
	return err
}

func (a *App) writeGroupListedMessageText(message GroupListedMessage) error {
	header := fmt.Sprintf(
		"message_id=%s group=%s person=%s read=%t read_count=%d eligible_count=%d created_at=%s subject=%q",
		message.MessageID,
		message.GroupAddress,
		message.Person,
		message.Read,
		message.ReadCount,
		message.EligibleCount,
		message.MessageCreatedAt,
		message.Subject,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	_, err := fmt.Fprintln(a.stdout, header)
	return err
}

func (a *App) writeGroupWaitedMessageText(message GroupListedMessageCompact) error {
	header := fmt.Sprintf(
		"message_id=%s group=%s person=%s read=%t read_count=%d eligible_count=%d content_type=%s subject=%q",
		message.MessageID,
		message.GroupAddress,
		message.Person,
		message.Read,
		message.ReadCount,
		message.EligibleCount,
		message.ContentType,
		message.Subject,
	)
	if message.ForwardedFromAddress != nil {
		header += fmt.Sprintf(" forwarded_from_address=%s", *message.ForwardedFromAddress)
	}
	if message.SenderAddress != nil {
		header += fmt.Sprintf(" sender_address=%s", *message.SenderAddress)
	}
	_, err := fmt.Fprintln(a.stdout, header)
	return err
}

func (a *App) newWatchEmitter(format outputFormat) (func(ListedDelivery) error, error) {
	switch format {
	case outputFormatText:
		return a.writeListedDeliveryText, nil
	case outputFormatJSON, outputFormatNDJSON:
		encoder := json.NewEncoder(a.stdout)
		return func(delivery ListedDelivery) error {
			return encoder.Encode(delivery)
		}, nil
	case outputFormatYAML:
		return func(delivery ListedDelivery) error {
			if _, err := io.WriteString(a.stdout, "---\n"); err != nil {
				return err
			}
			return writeYAML(a.stdout, delivery)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported watch output format %d", format)
	}
}
