package dev.buesing.ksd.publisher;

import com.beust.jcommander.Parameter;
import dev.buesing.ksd.common.domain.args.VersionValidator;
import dev.buesing.ksd.tools.config.BaseOptions;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Options extends BaseOptions {

    @Parameter(names = { "--line-items" }, description = "use x-y for a range, single value for absolute")
    private String lineItemCount= "1-3";

    @Parameter(names = { "--pause" }, description = "")
    private Long pause = 1000L;

    @Parameter(names = { "--skus" }, description = "")
    private List<Integer> skus;

    @Parameter(names = { "--stores" }, description = "")
    private List<Integer> stores;

    @Parameter(names = { "--max-sku" }, description = "")
    private int maxSku = getNumberOfProducts();

    @Parameter(names = {"-v", "--version"}, required = true, validateWith = VersionValidator.class, description = "application version v1 or v2")
    private String version;


}
