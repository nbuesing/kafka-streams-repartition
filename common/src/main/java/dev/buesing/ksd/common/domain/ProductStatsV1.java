package dev.buesing.ksd.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "$type")
public class ProductStatsV1 {

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class StoreQuantity {
        private int storeId;
        private int quantity;
        public void addToQuantity(final int quantity) {
            this.quantity += quantity;
        }
    }

    private String sku;
    private Integer quantity = 0;
    private Integer orders = 0;
    private List<StoreQuantity> quantityByStore = new ArrayList<>();

    // needed for serializers
    @SuppressWarnings("unused")
    private ProductStatsV1() {
    }

    public ProductStatsV1(final String sku) {
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

        final int storeId = Integer.parseInt(purchaseOrder.getStoreId());

        Optional<StoreQuantity> record = quantityByStore.stream().filter(p -> p.getStoreId() == storeId).findFirst();

        if (record.isPresent()) {
            record.get().addToQuantity(quantity);
        } else {
            this.quantityByStore.add(new StoreQuantity(storeId, quantity));
        }
    }

}
