package com.melloware.petstore.order.gateway.temporal;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.melloware.petstore.common.activities.order.OrderNotificationActivities;
import com.melloware.petstore.common.activities.order.OrderServiceActivities;
import com.melloware.petstore.common.activities.payment.PaymentActivities;
import com.melloware.petstore.common.activities.shipper.ShipperActivities;
import com.melloware.petstore.common.activities.warehouse.WarehouseActivities;
import com.melloware.petstore.common.models.enums.OrderFailureReason;
import com.melloware.petstore.common.models.exceptions.BadPaymentInfoException;
import com.melloware.petstore.common.models.exceptions.OutOfStockException;
import com.melloware.petstore.common.models.exceptions.PaymentDeclinedException;
import com.melloware.petstore.common.models.exceptions.PurchasingException;
import com.melloware.petstore.common.models.json.CheckInventoryRequest;
import com.melloware.petstore.common.models.json.CreateOrderRequest;
import com.melloware.petstore.common.models.json.CreateOrderResponse;
import com.melloware.petstore.common.models.json.CreateTrackingNumberRequest;
import com.melloware.petstore.common.models.json.DebitCreditCardRequest;
import com.melloware.petstore.common.models.json.DebitCreditCardResponse;
import com.melloware.petstore.common.models.json.MarkOrderCompleteRequest;
import com.melloware.petstore.common.models.json.MarkOrderFailedRequest;
import com.melloware.petstore.common.models.json.OrderErrorEmailNotificationRequest;
import com.melloware.petstore.common.models.json.OrderReceivedEmailNotificationRequest;
import com.melloware.petstore.common.models.json.OrderSuccessEmailNotificationRequest;
import com.melloware.petstore.common.models.json.Product;
import com.melloware.petstore.common.models.json.ReverseActionsForTransactionRequest;
import com.melloware.petstore.common.utils.TemporalActivityExceptionChecker;

import io.quarkiverse.temporal.TemporalActivityStub;
import io.quarkus.arc.ArcUndeclaredThrowableException;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Saga;
import io.temporal.workflow.Workflow;

import lombok.extern.jbosslog.JBossLog;

/**
 * Implementation of the OrderPurchaseWorkflow interface for simulating a
 * product order scenario.
 * <p>
 * This workflow orchestrates the following steps:
 * <ol>
 * <li>Receive and validate the order request</li>
 * <li>Send an order acknowledgement email</li>
 * <li>Create an initial order record in the database</li>
 * <li>Calculate the total order price</li>
 * <li>Process the payment by charging the customer's credit card</li>
 * <li>Check inventory availability</li>
 * <li>Generate a shipping tracking number</li>
 * <li>Finalize the order and send a confirmation email</li>
 * </ol>
 * <p>
 * The workflow includes error handling and compensation logic to manage
 * failures at various stages of the order process.
 */
@JBossLog
public class PurchaseOrderWorkflowImpl implements PurchaseOrderWorkflow {

    // Activity stubs are created by the extension when the workflow instance is created,
    // this is equivalent to Workflow.newActivityStub(...) in a field initializer.
    // Retries are limited for demo purposes.
    @TemporalActivityStub(taskQueue = "payment-tasks", startToCloseTimeout = "30s", cancellationType = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retryInitialInterval = "1s", retryMaximumInterval = "100s", retryBackoffCoefficient = 2, retryMaximumAttempts = 500, retryDoNotRetry = {
            ArcUndeclaredThrowableException.class, BadPaymentInfoException.class, ConstraintViolationException.class,
            JsonMappingException.class, NullPointerException.class, OutOfStockException.class,
            PaymentDeclinedException.class, PurchasingException.class, IllegalArgumentException.class })
    private PaymentActivities paymentActivity;

    @TemporalActivityStub(taskQueue = "notification-tasks", startToCloseTimeout = "30s", cancellationType = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retryInitialInterval = "1s", retryMaximumInterval = "100s", retryBackoffCoefficient = 2, retryMaximumAttempts = 500, retryDoNotRetry = {
            ArcUndeclaredThrowableException.class, BadPaymentInfoException.class, ConstraintViolationException.class,
            JsonMappingException.class, NullPointerException.class, OutOfStockException.class,
            PaymentDeclinedException.class, PurchasingException.class, IllegalArgumentException.class })
    private OrderNotificationActivities notificationActivity;

    @TemporalActivityStub(taskQueue = "order-tasks", startToCloseTimeout = "30s", cancellationType = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retryInitialInterval = "1s", retryMaximumInterval = "100s", retryBackoffCoefficient = 2, retryMaximumAttempts = 500, retryDoNotRetry = {
            ArcUndeclaredThrowableException.class, BadPaymentInfoException.class, ConstraintViolationException.class,
            JsonMappingException.class, NullPointerException.class, OutOfStockException.class,
            PaymentDeclinedException.class, PurchasingException.class, IllegalArgumentException.class })
    private OrderServiceActivities orderActivity;

    @TemporalActivityStub(taskQueue = "warehouse-tasks", startToCloseTimeout = "30s", cancellationType = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retryInitialInterval = "1s", retryMaximumInterval = "100s", retryBackoffCoefficient = 2, retryMaximumAttempts = 500, retryDoNotRetry = {
            ArcUndeclaredThrowableException.class, BadPaymentInfoException.class, ConstraintViolationException.class,
            JsonMappingException.class, NullPointerException.class, OutOfStockException.class,
            PaymentDeclinedException.class, PurchasingException.class, IllegalArgumentException.class })
    private WarehouseActivities warehouseActivity;

    @TemporalActivityStub(taskQueue = "shipment-tasks", startToCloseTimeout = "30s", cancellationType = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED, retryInitialInterval = "1s", retryMaximumInterval = "100s", retryBackoffCoefficient = 2, retryMaximumAttempts = 500, retryDoNotRetry = {
            ArcUndeclaredThrowableException.class, BadPaymentInfoException.class, ConstraintViolationException.class,
            JsonMappingException.class, NullPointerException.class, OutOfStockException.class,
            PaymentDeclinedException.class, PurchasingException.class, IllegalArgumentException.class })
    private ShipperActivities shipmentActivity;

    /**
     * Initiates and executes the order placement workflow.
     * <p>
     * This method orchestrates the entire order process, including validation,
     * payment processing, inventory checking, and order finalization. It also
     * handles error scenarios and initiates compensation actions when necessary.
     *
     * @param orderCtx The context object containing all necessary information for
     *                 the order.
     * @throws NullPointerException if orderCtx is null.
     * @throws TemporalFailure      for workflow-related failures.
     */
    @Override
    public void placeOrder(@Valid @NotNull PurchaseOrderContext orderCtx) {

        Objects.requireNonNull(orderCtx, "PurchaseOrderContext is required");

        // Initialize the saga for potential compensations
        Saga saga = new Saga(new Saga.Options.Builder().build());

        try {

            // 1. Send the acknowledgement of the order request
            sendOrderReceivedEmail(orderCtx);

            // 2. Create the initial order record
            CreateOrderResponse newOrder = generateProductOrder(orderCtx);

            // Set order number into context
            orderCtx = orderCtx.toBuilder()
                    .orderNumber(newOrder.getOrderNumber())
                    .status(newOrder.getStatus())
                    .build();

            // 3. Calculate the order total
            double orderTotal = calculateTotalPrice(orderCtx.getProducts());

            // Set the total into the context
            orderCtx = orderCtx
                    .toBuilder()
                    .orderTotal(orderTotal)
                    .build();

            // 4. Charge the credit card
            // This could throw an error for something like
            // INVALID_CARD_INFO, PAYMENT_DECLINED, etc.
            // In the REAL WORLD this would call out to a 3rd party service
            // We are just managing our own service for the demo
            debitCreditCard(saga, orderCtx);

            // 5. Check with warehouse to see if the products are in stock - fail if not
            // If this fails, we compensate the customers credit card and reverse the charge
            // For this demo we are choosing to check inventory after charging the customer
            // for the products.
            // In a REAL WORLD scenario, this might be done before charging the customer

            /** NOTE: Any exception after this point will cause the compensation to run **/
            CheckInventoryRequest invRequest = CheckInventoryRequest.builder()
                    .products(orderCtx.getProducts())
                    .build();

            warehouseActivity.checkInventory(invRequest);

            // 6. get the shipping information/tracking number from the shipper
            CreateTrackingNumberRequest trackRequest = CreateTrackingNumberRequest.builder()
                    .products(orderCtx.getProducts())
                    .build();

            // Add it into the order context
            String trackingNumber = shipmentActivity.createTrackingNumber(trackRequest);
            orderCtx = orderCtx.toBuilder()
                    .trackingNumber(trackingNumber)
                    .build();

            // 7. Save order history and send out email
            completeOrder(orderCtx);

        } catch (TemporalFailure e) {
            log.error(ExceptionUtils.getRootCauseMessage(e), e);
            PurchaseOrderContext finalOrderCtx = orderCtx; // Workaround for "effectively final" requirement
            Workflow.newDetachedCancellationScope(
                    () -> cleanup(e, saga, finalOrderCtx, finalOrderCtx.getTransactionId())).run();
            throw e;
        }
    }

    /**
     * Calculates the total price for a list of products based on quantity and
     * price.
     *
     * @param products List of products.
     * @return Total price.
     */
    private double calculateTotalPrice(List<Product> products) {
        return products.stream()
                .mapToDouble(product -> product.getQuantity() * product.getPrice())
                .sum();
    }

    /**
     * Creates a new order with some initial information from the
     * request.
     * <p>
     * The rest of the data will be added later in the process
     *
     * @param purchaseCtx {@link PurchaseOrderContext}
     * @return {@link CreateOrderResponse}
     */
    private CreateOrderResponse generateProductOrder(PurchaseOrderContext purchaseCtx) {
        log.infof("Generating new order record for TX id %s", purchaseCtx.getTransactionId());
        CreateOrderRequest newOrderReq = CreateOrderRequest.builder()
                .customerEmail(purchaseCtx.getCustomerEmail())
                .orderDate(purchaseCtx.getRequestDate())
                .transactionId(purchaseCtx.getTransactionId())
                .requestedByHost(purchaseCtx.getRequestedByHost())
                .requestedByUser(purchaseCtx.getRequestedByUser())
                .products(purchaseCtx.getProducts())
                .build();

        return orderActivity.createOrder(newOrderReq);
    }

    /**
     * Sends the order received email
     *
     * @param purchaseCtx {@link PurchaseOrderContext}
     */
    private void sendOrderReceivedEmail(PurchaseOrderContext purchaseCtx) {
        log.info("Sending order request received notification");
        OrderReceivedEmailNotificationRequest orderRcvReq = OrderReceivedEmailNotificationRequest.builder()
                .transactionNumber(purchaseCtx.getTransactionId())
                .customerEmail(purchaseCtx.getCustomerEmail())
                .orderDate(purchaseCtx.getRequestDate())
                .products(purchaseCtx.getProducts())
                .build();

        notificationActivity.sendOrderReceivedEmail(orderRcvReq);
    }

    /**
     * Performs compensations and other cleanup operations in case of workflow
     * failure.
     *
     * This method is responsible for:
     * 1. Executing compensation actions defined in the Saga.
     * 2. Handling order failure if the exception is not a cancellation.
     * 3. Logging the cleanup process.
     *
     * @param e             The exception that triggered the cleanup.
     * @param saga          The Saga object containing compensation actions.
     * @param ctx           The OrderPurchaseContext containing order details.
     * @param transactionId The UUID of the transaction being cleaned up.
     */
    private void cleanup(Exception e, Saga saga, PurchaseOrderContext ctx, UUID transactionId) {
        log.infof("Performing cleanup operations for TX id %s", transactionId);

        // Execute compensation actions
        try {
            if (saga != null) {
                saga.compensate();
            }
        } catch (Exception cpe) {
            log.error("Failed to complete compensations!", cpe);
        }

        // Cancelled failures are when the workflow is cancelled
        // from the WebUI or through code
        // You could handle that situation in your workflow
        // if you allowed cancellations..in this demo we do not
        if (!(e instanceof CanceledFailure)) {
            if (ctx != null) {
                failOrder(e, ctx);
            }
        }

        log.infof("Finished cleanup operations for TX id %s", transactionId);
    }

    /**
     * Performs operations when a order fails
     *
     * @param e   The exception that caused the order to fail
     * @param ctx {@link PurchaseOrderContext}
     */
    private void failOrder(Exception e, PurchaseOrderContext ctx) {

        // Default to SYSTEM error
        OrderFailureReason reason = OrderFailureReason.SYSTEM_ERROR;

        // Check for specific errors
        if (TemporalActivityExceptionChecker.isExceptionType(e, PaymentDeclinedException.class)) {
            reason = OrderFailureReason.PAYMENT_DECLINED;
        } else if (TemporalActivityExceptionChecker.isExceptionType(e, BadPaymentInfoException.class)) {
            reason = OrderFailureReason.INVALID_PAYMENT_METHOD;
        } else if (TemporalActivityExceptionChecker.isExceptionType(e, OutOfStockException.class)) {
            reason = OrderFailureReason.OUT_OF_STOCK_ITEMS;
        }

        log.infof("Marking order as failed with TX id %s", ctx.getTransactionId());

        // Mark the order as failed
        MarkOrderFailedRequest req = MarkOrderFailedRequest.builder()
                .orderNumber(ctx.getOrderNumber())
                .transactionId(ctx.getTransactionId())
                .reason(reason)
                .build();

        // Call activity
        orderActivity.markOrderAsFailed(req);

        // send error email
        OrderErrorEmailNotificationRequest emailRequest = OrderErrorEmailNotificationRequest.builder()
                .orderDate(ctx.getRequestDate())
                .customerEmail(ctx.getCustomerEmail())
                .orderNumber(ctx.getOrderNumber())
                .transactionNumber(ctx.getTransactionId())
                .build();

        // Call activity to send email
        notificationActivity.sendOrderErrorEmail(emailRequest);
    }

    /**
     * Performs operations when an order completes successfully
     *
     * @param ctx {@link PurchaseOrderContext}
     */
    private void completeOrder(PurchaseOrderContext ctx) {

        log.infof("Marking order %s as complete with TX id %s", ctx.getOrderNumber(), ctx.getTransactionId());

        /** Save the final order to the database **/
        MarkOrderCompleteRequest completeReq = MarkOrderCompleteRequest.builder()
                .products(ctx.getProducts())
                .orderDate(ctx.getRequestDate())
                .orderNumber(ctx.getOrderNumber())
                .transactionId(ctx.getTransactionId())
                .customerEmail(ctx.getCustomerEmail())
                .orderTotal(ctx.getOrderTotal())
                .build();

        orderActivity.markOrderAsComplete(completeReq);

        /** Send NOTIFICATION ***/
        // Create request
        OrderSuccessEmailNotificationRequest emailRequest = OrderSuccessEmailNotificationRequest.builder()
                .customerEmail(ctx.getCustomerEmail())
                .transactionNumber(ctx.getTransactionId())
                .orderNumber(ctx.getOrderNumber())
                .orderDate(ctx.getRequestDate())
                .products(ctx.getProducts())
                .trackingNumber(ctx.getTrackingNumber())
                .orderTotal(ctx.getOrderTotal())
                .build();

        // Call activity to send email
        log.infof("Order updated..Sending notification email to %s", ctx.getCustomerEmail());
        notificationActivity.sendOrderSuccessEmail(emailRequest);
    }

    /**
     * Process the credit card specified in the order context
     *
     * @param saga Saga to add compensation actions
     * @param ctx  {@link PurchaseOrderContext}
     * @return {@link DebitCreditCardResponse} containing the result of the credit
     *         card transaction
     */
    private DebitCreditCardResponse debitCreditCard(Saga saga, PurchaseOrderContext ctx) {

        log.info("Calling Payment service to debit credit card");

        // Setup the compensation
        ReverseActionsForTransactionRequest reverseRequest = ReverseActionsForTransactionRequest.builder()
                .requestedByHost(ctx.getRequestedByHost())
                .requestedByUser(ctx.getRequestedByUser())
                .transactionId(ctx.getTransactionId())
                .build();

        // Create the reversal in case of compensations later
        saga.addCompensation(() -> paymentActivity.reversePaymentTransactions(reverseRequest));

        // debit card and return some sort of auth number or whatever
        DebitCreditCardRequest cardRequest = DebitCreditCardRequest.builder()
                .amount(ctx.getOrderTotal())
                .creditCard(ctx.getCreditCard())
                .customerEmail(ctx.getCustomerEmail())
                .transactionId(ctx.getTransactionId())
                .requestedByHost(ctx.getRequestedByHost())
                .requestedByUser(ctx.getRequestedByUser())
                .build();

        return paymentActivity.debitCreditCard(cardRequest);

    }
}