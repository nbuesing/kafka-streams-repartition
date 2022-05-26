package dev.buesing.ksd.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "$type")
public class ProductStatsV2 {

    private String sku;
    private long quantity = 0L;
    private int orders = 0;
    private Map<String, Long> quantityByStore = new HashMap<>();

    // needed for serializers
    @SuppressWarnings("unused")
    private ProductStatsV2() {
    }

    public ProductStatsV2(final String sku) {
        this.sku = sku;
    }

    public void add(final PurchaseOrder purchaseOrder) {

        final int quantity = purchaseOrder.getQuantity(sku);

        // just in case quantity of 0 does make it into order, do not count it.
        if (quantity == 0) {
            return;
        }

        this.orders += 1;
        this.quantity += quantity;

        quantityByStore.put(purchaseOrder.getStoreId(), (long) quantityByStore.getOrDefault(purchaseOrder.getStoreId(), 0L) + quantity);
    }

    public void merge(final ProductStatsV1 productStatsV1) {

        this.orders += productStatsV1.getOrders();
        this.quantity += productStatsV1.getQuantity();

        productStatsV1.getQuantityByStore().stream().forEach(storeQuantity -> {
            final String storeId = Integer.toString(storeQuantity.getStoreId());
            quantityByStore.put(Integer.toString(storeQuantity.getStoreId()), quantityByStore.getOrDefault(storeId, 0L) + (long) storeQuantity.getQuantity());
        });
    }
}
