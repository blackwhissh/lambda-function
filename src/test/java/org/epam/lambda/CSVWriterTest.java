package org.epam.lambda;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CSVWriterTest {
    private static final String FIRST_NAME = "trainer_first_name";
    private static final String LAST_NAME = "trainer_last_name";
    private static final String STATUS = "trainee_status";
    private StringWriter writer;
    private List<Item> items;
    private LocalDate now;

    @BeforeEach
    void setUp() {
        writer = new StringWriter();
        items = new ArrayList<>();
        now = LocalDate.now();
    }

    @Test
    void testWriteCSV_ActiveItems() {
        Item activeItem = mock(Item.class);
        when(activeItem.getString(FIRST_NAME)).thenReturn("John");
        when(activeItem.getString(LAST_NAME)).thenReturn("Doe");
        when(activeItem.getString(STATUS)).thenReturn("active");

        Map<String, Object> yearData = new HashMap<>();
        Map<String, Object> monthData = new HashMap<>();
        monthData.put("" + now.getMonthValue(), 5);
        yearData.put("" + now.getYear(), monthData);
        when(activeItem.getList("years")).thenReturn(Collections.singletonList(yearData));
        when(activeItem.getList("years").get(0)).thenReturn(Collections.singletonList(monthData));
        items.add(activeItem);

        ReportGenerator.writeCSV(writer, items, now);

        assertFalse(writer.toString().isEmpty());
    }
}
