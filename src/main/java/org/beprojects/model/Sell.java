package org.beprojects.model;

import java.math.BigDecimal;
public class Sell {
    private Long operationId;
    private Long clientId;
    private Integer ticketAmount;
    private BigDecimal totalValue;
    private String ticketStatus;
    private Sell(){
    }
    public Sell(Long operationId, Long clientId, Integer ticketAmount, BigDecimal totalValue) {
        this.operationId = operationId;
        this.clientId = clientId;
        this.ticketAmount = ticketAmount;
        this.totalValue = totalValue;
    }

    public Long getOperationId() {
        return operationId;
    }

    public void setOperationId(Long operationId) {
        this.operationId = operationId;
    }

    public Long getClientId() {
        return clientId;
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }

    public Integer getTicketAmount() {
        return ticketAmount;
    }

    public void setTicketAmount(Integer ticketAmount) {
        this.ticketAmount = ticketAmount;
    }

    public BigDecimal getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(BigDecimal totalValue) {
        this.totalValue = totalValue;
    }

    public String getTicketStatus() {
        return ticketStatus;
    }

    public void setTicketStatus(String ticketStatus) {
        this.ticketStatus = ticketStatus;
    }

    @Override
    public String toString() {
        return "Sell{" +
                "operationId=" + operationId +
                ", clientId=" + clientId +
                ", ticketAmount=" + ticketAmount +
                ", totalValue=" + totalValue +
                ", ticketStatus='" + ticketStatus + '\'' +
                '}';
    }
}
